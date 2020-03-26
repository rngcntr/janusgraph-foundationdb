package com.experoinc.janusgraph.diskstorage.foundationdb;

import static java.util.AbstractMap.SimpleEntry;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
public class FoundationDBTx extends AbstractStoreTransaction {

    private static final Logger log = LoggerFactory.getLogger(FoundationDBTx.class);

    private volatile Transaction tx;

    private final Database db;

    private List<SimpleEntry<byte[], byte[]>> inserts = new LinkedList<>();
    private List<byte[]> deletions = new LinkedList<>();

    private long maxRuns = 1;

    public enum IsolationLevel { SERIALIZABLE, READ_COMMITTED_NO_WRITE, READ_COMMITTED_WITH_WRITE }

    private final IsolationLevel isolationLevel;

    private AtomicInteger txCtr = new AtomicInteger(0);
    private boolean hasCompletedReadOperation = false;

    public FoundationDBTx(Database db, Transaction t, BaseTransactionConfig config,
                          IsolationLevel isolationLevel, long maxRuns) {
        super(config);
        tx = t;
        this.db = db;
        this.isolationLevel = isolationLevel;

        switch (isolationLevel) {
        case SERIALIZABLE:
            this.maxRuns = 1;
            break;
        case READ_COMMITTED_NO_WRITE:
        case READ_COMMITTED_WITH_WRITE:
            this.maxRuns = maxRuns;
        }

    }

    public synchronized void restart() throws FoundationDBTxException {
        txCtr.incrementAndGet();
        if (tx == null) {
            return;
        }

        try {
            tx.cancel();
        } catch (IllegalStateException ignored) {
        } finally {
            tx.close();
        }

        if (isolationLevel == IsolationLevel.SERIALIZABLE && hasCompletedReadOperation) {
            // only retry this transaction if it has not exposed any data yet
            throw new FoundationDBTxException(FoundationDBTxException.TIMEOUT);
        }

        tx = db.createTransaction();

        /*
         * Reapply mutations but do not clear them out just in case this transaction also
         * times out and they need to be reapplied.
         * 
         * @todo Note that at this point, the large transaction case (tx exceeds 10,000,000 bytes)
         * is not handled.
         */
        inserts.forEach(insert -> {
            tx.set(insert.getKey(), insert.getValue());
        });

        deletions.forEach(delete -> {
            tx.clear(delete);
        });
    }

    @Override
    public synchronized void rollback() throws BackendException {
        super.rollback();
        if (tx == null) {
            return;
        }

        if (log.isTraceEnabled()) {
            log.trace("{} rolled back", this.toString(),
                      new FoundationDBTx.TransactionClosed(this.toString()));
        }

        try {
            tx.cancel();
            tx.close();
            tx = null;
        } catch (Exception e) {
            throw new FoundationDBTxException(e);
        } finally {
            if (tx != null) {
                tx.close();
            }
        }
    }

    @Override
    public synchronized void commit() throws BackendException {
        boolean failing = true;
        for (int i = 0; i < maxRuns; i++) {
            super.commit();

            if (tx == null) {
                return;
            }

            if (log.isTraceEnabled()) {
                log.trace("{} committed", this.toString(),
                          new FoundationDBTx.TransactionClosed(this.toString()));
            }

            try {
                if (!inserts.isEmpty() || !deletions.isEmpty()) {
                    tx.commit().get();
                } else {
                    // nothing to commit so skip it
                    tx.cancel();
                }
                tx.close();
                tx = null;
                failing = false;
                break;
            } catch (IllegalStateException | ExecutionException e) {
                restart();
            } catch (Exception e) {
                throw new FoundationDBTxException(e);
            }
        }

        if (failing) {
            throw new FoundationDBTxException(FoundationDBTxException.TIMEOUT);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + (null == tx ? "nulltx" : tx.toString());
    }

    private static class TransactionClosed extends Exception {
        private static final long serialVersionUID = 1L;
        private TransactionClosed(String msg) { super(msg); }
    }

    /**
     * Fetches the value for a given key from the KV store.
     * 
     * @param key The key at which the requested value is located.
     * @return The value for the given key.
     * @throws FoundationDBTxException If the read operation can not be completed.
     */
    public byte[] get(final byte[] key) throws FoundationDBTxException {
        byte[] result = waitForFuture(readWithRetriesAsync(readTx -> readTx.get(key)));

        synchronized (this) {
            hasCompletedReadOperation = true;
            return result;
        }
    }

    /**
     * Fetches the values for a given key range from the KV store.
     * 
     * @param query The range query.
     * @return The values for the given key range.
     * @throws FoundationDBTxException If the read operation can not be completed.
     */
    public List<KeyValue> getRange(final FoundationDBRangeQuery query)
        throws FoundationDBTxException {
        List<KeyValue> result = waitForFuture(
            readWithRetriesAsync(readTx
                                 -> readTx
                                        .getRange(query.getStartKeySelector(),
                                                  query.getEndKeySelector(), query.getLimit())
                                        .asList()));
        synchronized (this) {
            hasCompletedReadOperation = true;
            return result != null ? result : Collections.emptyList();
        }
    }

    /**
     * Fetches the values for multiple key ranges from the KV store.
     * 
     * @param query The range queries.
     * @return The values for the given key ranges.
     * @throws FoundationDBTxException If the read operation can not be completed.
     */
    public Map<KVQuery, List<KeyValue>>
    getMultiRange(final Collection<FoundationDBRangeQuery> queries) throws FoundationDBTxException {
        Map<KVQuery, CompletableFuture<List<KeyValue>>> futureMap = new ConcurrentHashMap<>();
        for (FoundationDBRangeQuery query : queries) {
            futureMap.put(
                query.asKVQuery(),
                readWithRetriesAsync(readTx
                                     -> readTx
                                            .getRange(query.getStartKeySelector(),
                                                      query.getEndKeySelector(), query.getLimit())
                                            .asList()));
        }

        Map<KVQuery, List<KeyValue>> resultMap = new ConcurrentHashMap<>();
        for (Entry<KVQuery, CompletableFuture<List<KeyValue>>> entry : futureMap.entrySet()) {
            resultMap.put(entry.getKey(), waitForFuture(entry.getValue()));
        }

        synchronized (this) {
            hasCompletedReadOperation = true;
            return resultMap;
        }
    }


    /**
     * Inserts a KV pair into the KV store. The change will take effect on commit time.
     *
     * @param key The key to insert.
     * @param value The value to insert.
     * @throws FoundationDBTxException If the transaction was concurrently closed by another thread.
     */
    public void set(final byte[] key, final byte[] value) throws FoundationDBTxException {
        inserts.add(new SimpleEntry<byte[], byte[]>(key, value));
        getWriteTransaction().set(key, value);
    }

    /**
     * Removes a key from the KV store. The change will take effect at commit time.
     *
     * @param key The key to remove.
     * @throws FoundationDBTxException If the transaction was concurrently closed by another thread.
     */
    public void clear(final byte[] key) throws FoundationDBTxException {
        deletions.add(key);
        getWriteTransaction().clear(key);
    }

    /**
     * Blocks until the future is completed either exceptionally or with a valid result.
     * 
     * @param <T> The return type of the future.
     * @param future The future to wait for.
     * @return The future's result if completion was successful.
     * @throws FoundationDBTxException If the future completed exceptionally.
     */
    private <T> T waitForFuture(CompletableFuture<T> future) throws FoundationDBTxException {
        try {
            return future.join();
        } catch (CompletionException cex) {
            try {
                throw cex.getCause();
            } catch (FDBException fdbex) {
                throw new FoundationDBTxException(FoundationDBTxException.TIMEOUT, fdbex);
            } catch (ExecutionException eex) {
                if (eex.getCause() instanceof FoundationDBTxException) {
                    throw (FoundationDBTxException) eex.getCause();
                } else {
                    throw new FoundationDBTxException(eex);
                }
            } catch (InterruptedException | IllegalStateException e) {
                throw new FoundationDBTxException(FoundationDBTxException.INTERRUPTED);
            } catch (Throwable impossible) {
                throw new AssertionError(impossible);
            }
        }
    }

    /**
     * Performs a read access on the KV store. If it fails, the read is retried up to maxRuns times.
     * If the last repetition fails, the returned future will complete exceptionally.
     * 
     * @param <T> The return type of the future.
     * @param operation The read to perform, as a function of a transaction.
     * @return The future that will eventually contain the result of the read.
     */
    private <T> CompletableFuture<T> readWithRetriesAsync(
        Function<? super ReadTransaction, ? extends CompletableFuture<T>> operation)
        throws FoundationDBTxException {
        int[] startTxId = {txCtr.get()};
        CompletableFuture<T> future;

        future = operation.apply(getReadTransaction());
        for (int i = 1; i < maxRuns; ++i) {
            future = future.exceptionally(th -> {
                if (txCtr.get() == startTxId[0]) {
                    try {
                        this.restart();
                    } catch (FoundationDBTxException fdbtex) {
                        throw new CompletionException(fdbtex);
                    }
                }
                startTxId[0] = txCtr.get();
                try {
                    return operation.apply(getReadTransaction()).join();
                } catch (FoundationDBTxException fdbtex) {
                    throw new CompletionException(fdbtex);
                }
            });
        }

        return future;
    }

    /**
     * Grants safe access to the underlying transaction. If the isolation level is not serializable,
     * a snapshot of the real transaction is returned, which reduces conflicts but can also cause
     * inconsistend reads.
     *
     * @return A read-only transaction, if the FoundationDB transaction is not null.
     * @throws FoundationDBTxException If the FoundationDB transaction is null.
     */
    private synchronized ReadTransaction getReadTransaction() throws FoundationDBTxException {
        if (tx == null) {
            throw new FoundationDBTxException(FoundationDBTxException.CLOSED_WHILE_ACTIVE);
        }
        return isolationLevel == IsolationLevel.SERIALIZABLE ? tx : tx.snapshot();
    }

    /**
     * Grants safe access to the underlying transaction.
     * 
     * @return The FoundationDB transaction, if not null.
     * @throws FoundationDBTxException If the FoundationDB transaction is null.
     */
    private synchronized Transaction getWriteTransaction() throws FoundationDBTxException {
        if (tx == null) {
            throw new FoundationDBTxException(FoundationDBTxException.CLOSED_WHILE_ACTIVE);
        }
        return tx;
    }
}
