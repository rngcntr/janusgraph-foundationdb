package com.experoinc.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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

    private List<Insert> inserts = new LinkedList<>();
    private List<byte[]> deletions = new LinkedList<>();

    private int maxRuns = 1;

    public enum IsolationLevel { SERIALIZABLE, READ_COMMITTED_NO_WRITE, READ_COMMITTED_WITH_WRITE }

    private final IsolationLevel isolationLevel;

    private AtomicInteger txCtr = new AtomicInteger(0);

    public FoundationDBTx(Database db, Transaction t, BaseTransactionConfig config,
                          IsolationLevel isolationLevel) {
        super(config);
        tx = t;
        this.db = db;
        this.isolationLevel = isolationLevel;

        switch (isolationLevel) {
        case SERIALIZABLE:
            break; // no retries
        case READ_COMMITTED_NO_WRITE:
        case READ_COMMITTED_WITH_WRITE:
            maxRuns = 3;
        }
    }

    public synchronized void restart() {
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

        tx = db.createTransaction();

        // Reapply mutations but do not clear them out just in case this transaction also
        // times out and they need to be reapplied.
        //
        // @todo Note that at this point, the large transaction case (tx exceeds 10,000,000 bytes)
        // is not handled.

        inserts.forEach(insert -> {
            tx.addReadConflictKey(insert.getKey());
            tx.set(insert.getKey(), insert.getValue());
        });

        deletions.forEach(delete -> {
            tx.addReadConflictKey(delete);
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
                      new FoundationDBTx.TransactionClose(this.toString()));
        }

        try {
            tx.cancel();
            tx.close();
            tx = null;
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        } finally {
            if (tx != null)
                tx.close();
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
                          new FoundationDBTx.TransactionClose(this.toString()));
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

    private static class TransactionClose extends Exception {
        private static final long serialVersionUID = 1L;

        private TransactionClose(String msg) { super(msg); }
    }

    public byte[] get(final byte[] key) throws PermanentBackendException {
        return runWithRetries(readTx -> readTx.get(key));
    }

    private interface RetriableOperation<T> {
        public CompletableFuture<T> read(ReadTransaction readTx) throws ExecutionException;
    }

    private <T> T runWithRetries (RetriableOperation<T> operation) throws PermanentBackendException {
        for (int i = 0; i < maxRuns; i++) {
            final int startTxId = txCtr.get();

            ReadTransaction readTx = isolationLevel == IsolationLevel.SERIALIZABLE ? tx : tx.snapshot();

            try {
                return operation.read(readTx).get();
            } catch (ExecutionException e) {
                if (txCtr.get() == startTxId) {
                    this.restart();
                }
            } catch (Exception e) {
                throw new PermanentBackendException(e);
            }
        }

        throw new PermanentBackendException("Max transaction reset count exceeded");
    }

    public List<KeyValue> getRange(final FoundationDBRangeQuery query)
        throws PermanentBackendException {
        List<KeyValue> result = runWithRetries(
            readTx -> readTx.getRange(query.getStartKey(), query.getEndKey(), query.getLimit()).asList());
        return result != null ? result : Collections.emptyList();
    }

    public synchronized Map<KVQuery, List<KeyValue>> getMultiRange(final List<FoundationDBRangeQuery> queries)
        throws PermanentBackendException {
        Map<KVQuery, List<KeyValue>> resultMap = new ConcurrentHashMap<>();

        for (FoundationDBRangeQuery query : queries) {
            resultMap.put(
                query.asKVQuery(),
                runWithRetries(
                    readTx
                    -> readTx.getRange(query.getStartKey(), query.getEndKey(), query.getLimit())
                           .asList()));
        }

        return resultMap;
    }

    public void set(final byte[] key, final byte[] value) {
        inserts.add(new Insert(key, value));
        tx.addReadConflictKey(key);
        tx.set(key, value);
    }

    public void clear(final byte[] key) {
        deletions.add(key);
        tx.addReadConflictKey(key);
        tx.clear(key);
    }

    private class Insert {
        private byte[] key;
        private byte[] value;

        public Insert(final byte[] key, final byte[] value) {
            this.key = key;
            this.value = value;
        }

        public byte[] getKey() { return this.key; }

        public byte[] getValue() { return this.value; }
    }
}
