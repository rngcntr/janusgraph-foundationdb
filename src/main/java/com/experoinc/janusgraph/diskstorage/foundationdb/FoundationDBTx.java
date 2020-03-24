package com.experoinc.janusgraph.diskstorage.foundationdb;

import static java.util.AbstractMap.SimpleEntry;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
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
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
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

    public synchronized void restart() throws PermanentBackendException {
        txCtr.incrementAndGet();
        if (tx == null) {
            return;
        }

        try {
            tx.cancel();
        } catch (IllegalStateException e) {
            //
        } finally {
            tx.close();
        }

        if (isolationLevel == IsolationLevel.SERIALIZABLE && hasCompletedReadOperation) {
            // only retry this transaction if it has not exposed any data yet
            throw new PermanentBackendException(
                "Transaction can not be retried because of earlier successful read");
        }

        tx = db.createTransaction();

        // Reapply mutations but do not clear them out just in case this transaction also
        // times out and they need to be reapplied.
        //
        // @todo Note that at this point, the large transaction case (tx exceeds 10,000,000 bytes)
        // is not handled.

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
            throw new PermanentBackendException(e);
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
                e.printStackTrace();
                if (isolationLevel.equals(IsolationLevel.SERIALIZABLE) ||
                    isolationLevel.equals(IsolationLevel.READ_COMMITTED_NO_WRITE)) {
                    break;
                }
                restart();
            } catch (Exception e) {
                throw new PermanentBackendException(e);
            }
        }

        if (failing) {
            throw new PermanentBackendException("Max transaction reset count exceeded");
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

    public byte[] get(final byte[] key) throws PermanentBackendException {
        byte[] result = waitForFuture(readWithRetriesAsync(readTx -> readTx.get(key)));
        hasCompletedReadOperation = true;
        return result;
    }

    public List<KeyValue> getRange(final FoundationDBRangeQuery query)
        throws PermanentBackendException {
        List<KeyValue> result = waitForFuture(readWithRetriesAsync(
            readTx
            -> readTx.getRange(query.getStartKey(), query.getEndKey(), query.getLimit()).asList()));
        hasCompletedReadOperation = true;
        return result != null ? result : Collections.emptyList();
    }

    public synchronized Map<KVQuery, List<KeyValue>>
    getMultiRange(final List<FoundationDBRangeQuery> queries) throws PermanentBackendException {
        Map<KVQuery, CompletableFuture<List<KeyValue>>> futureMap = new ConcurrentHashMap<>();
        for (FoundationDBRangeQuery query : queries) {
            futureMap.put(
                query.asKVQuery(),
                readWithRetriesAsync(
                    readTx
                    -> readTx.getRange(query.getStartKey(), query.getEndKey(), query.getLimit())
                           .asList()));
        }

        Map<KVQuery, List<KeyValue>> resultMap = new ConcurrentHashMap<>();
        for (Entry<KVQuery, CompletableFuture<List<KeyValue>>> entry : futureMap.entrySet()) {
            resultMap.put(entry.getKey(), waitForFuture(entry.getValue()));
        }

        hasCompletedReadOperation = true;
        return resultMap;
    }

    public void set(final byte[] key, final byte[] value) {
        inserts.add(new SimpleEntry<byte[], byte[]>(key, value));
        tx.set(key, value);
    }

    public void clear(final byte[] key) {
        deletions.add(key);
        tx.clear(key);
    }

    private interface RetriableOperation<T> {
        public CompletableFuture<T> read(ReadTransaction readTx);
    }

    private <T> T waitForFuture(CompletableFuture<T> future) throws PermanentBackendException {
        try {
            return future.get();
        } catch (ExecutionException eex) {
            eex.printStackTrace();
            throw new PermanentBackendException("Max transaction reset count exceeded");
        } catch (InterruptedException e) {
            throw new PermanentBackendException(
                "Interrupted while waiting for FoundationDB result");
        }
    }

    private <T> CompletableFuture<T> readWithRetriesAsync(RetriableOperation<T> operation)
        throws PermanentBackendException {
        int[] startTxId = {txCtr.get()};
        CompletableFuture<T> future;

        try {
            future = operation.read(getReadTransaction());
            for (int i = 1; i < maxRuns; ++i) {
                future = future.exceptionally(th -> {
                    if (txCtr.get() == startTxId[0]) {
                        try {
                            this.restart();
                        } catch (PermanentBackendException pbex) {
                            throw new CompletionException(pbex);
                        }
                    }
                    startTxId[0] = txCtr.get();
                    try {
                        return operation.read(getReadTransaction()).join();
                    } catch (TransactionClosed tc) {
                        throw new CompletionException(tc);
                    }
                });
            }
        } catch (TransactionClosed tc) {
            throw new PermanentBackendException(tc);
        }

        return future;
    }

    private synchronized ReadTransaction getReadTransaction() throws TransactionClosed {
        if (tx == null) {
            throw new TransactionClosed("Transaction closed during execution");
        }

        return isolationLevel == IsolationLevel.SERIALIZABLE ? tx : tx.snapshot();
    }
}
