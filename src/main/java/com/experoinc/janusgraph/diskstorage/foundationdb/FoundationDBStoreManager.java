// Copyright 2018 Expero Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.experoinc.janusgraph.diskstorage.foundationdb;

import static com.experoinc.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.*;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.experoinc.janusgraph.diskstorage.foundationdb.FoundationDBTx.IsolationLevel;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.common.AbstractStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Experimental FoundationDB storage manager implementation.
 *
 * @author Ted Wilmes (twilmes@gmail.com)
 */
public class FoundationDBStoreManager
    extends AbstractStoreManager implements OrderedKeyValueStoreManager {

    private static final Logger log = LoggerFactory.getLogger(FoundationDBStoreManager.class);

    private final Map<String, FoundationDBKeyValueStore> stores;

    private FDB fdb;
    private Database db;
    private final StoreFeatures features;
    private DirectorySubspace rootDirectory;
    private final String rootDirectoryName;
    private final FoundationDBTx.IsolationLevel isolationLevel;
    private long retryCount;

    public FoundationDBStoreManager(Configuration configuration) throws BackendException {
        super(configuration);
        stores = new ConcurrentHashMap<>();

        fdb = FDB.selectAPIVersion(configuration.get(VERSION));
        rootDirectoryName = determineRootDirectoryName(configuration);
        db = FoundationDBConfigOptions.CLUSTER_FILE_PATH.getDefaultValue().equals(
                 configuration.get(CLUSTER_FILE_PATH))
                 ? fdb.open()
                 : fdb.open(configuration.get(CLUSTER_FILE_PATH));

        retryCount = configuration.get(TRANSACTION_RETRIES);

        try {
            isolationLevel = FoundationDBTx.IsolationLevel.valueOf(
                configuration.get(ISOLATION_LEVEL).toUpperCase().trim());
        } catch (IllegalArgumentException iaex) {
            throw new PermanentBackendException("Unrecognized isolation level " +
                                                configuration.get(ISOLATION_LEVEL));
        }

        initialize(rootDirectoryName);

        features = new StandardStoreFeatures.Builder()
                       .orderedScan(true)
                       .transactional(transactional)
                       .keyConsistent(GraphDatabaseConfiguration.buildGraphConfiguration())
                       .locking(true)
                       .keyOrdered(true)
                       .supportsInterruption(false)
                       .optimisticLocking(true)
                       .multiQuery(true)
                       .cellTTL(false)
                       .storeTTL(false)
                       .build();
    }

    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    private void initialize(final String directoryName) throws BackendException {
        try {
            // create the root directory to hold the JanusGraph data
            rootDirectory =
                DirectoryLayer.getDefault().createOrOpen(db, PathUtil.from(directoryName)).get();
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public StoreTransaction beginTransaction(final BaseTransactionConfig txCfg)
        throws BackendException {
        try {
            final Transaction tx = db.createTransaction();
            final StoreTransaction fdbTx =
                new FoundationDBTx(db, tx, txCfg, isolationLevel, retryCount);

            if (log.isTraceEnabled()) {
                log.trace("FoundationDB tx created", new TransactionBegin(fdbTx.toString()));
            }

            return fdbTx;
        } catch (Exception e) {
            throw new PermanentBackendException("Could not start FoundationDB transaction", e);
        }
    }

    @Override
    public FoundationDBKeyValueStore openDatabase(String name) throws BackendException {
        Preconditions.checkNotNull(name);
        if (stores.containsKey(name)) {
            return stores.get(name);
        }
        try {
            final DirectorySubspace storeDb =
                rootDirectory.createOrOpen(db, PathUtil.from(name)).get();
            log.debug("Opened database {}", name, new Throwable());

            FoundationDBKeyValueStore store = new FoundationDBKeyValueStore(name, storeDb, this);
            stores.put(name, store);
            return store;
        } catch (Exception e) {
            throw new PermanentBackendException("Could not open FoundationDB data store", e);
        }
    }

    @Override
    public void mutateMany(Map<String, KVMutation> mutations, StoreTransaction txh)
        throws BackendException {
        for (Map.Entry<String, KVMutation> mutation : mutations.entrySet()) {
            FoundationDBKeyValueStore store = openDatabase(mutation.getKey());
            KVMutation mutationValue = mutation.getValue();

            if (!mutationValue.hasAdditions() && !mutationValue.hasDeletions()) {
                log.debug("Empty mutation set for {}, doing nothing", mutation.getKey());
            } else {
                log.debug("Mutating {}", mutation.getKey());
            }

            if (mutationValue.hasAdditions()) {
                for (KeyValueEntry entry : mutationValue.getAdditions()) {
                    store.insert(entry.getKey(), entry.getValue(), txh, null);
                    log.trace("Insertion on {}: {}", mutation.getKey(), entry);
                }
            }

            if (mutationValue.hasDeletions()) {
                for (StaticBuffer del : mutationValue.getDeletions()) {
                    store.delete(del, txh);
                    log.trace("Deletion on {}: {}", mutation.getKey(), del);
                }
            }
        }
    }

    void removeDatabase(FoundationDBKeyValueStore db) {
        if (!stores.containsKey(db.getName())) {
            throw new IllegalArgumentException(
                "Tried to remove an unkown database from the storage manager");
        }

        String name = db.getName();
        stores.remove(name);
        log.debug("Removed database {}", name);
    }

    @Override
    public void close() throws BackendException {
        if (fdb != null) {
            if (!stores.isEmpty()) {
                throw new IllegalStateException(
                    "Cannot shutdown manager since some databases are still open");
            }

            try {
                db.close();
            } catch (Exception e) {
                throw new PermanentBackendException("Could not close FoundationDB database", e);
            }
        }
    }

    @Override
    public void clearStorage() throws BackendException {
        try {
            rootDirectory.removeIfExists(db).get();
        } catch (Exception e) {
            throw new PermanentBackendException("Could not clear FoundationDB storage", e);
        }
    }

    @Override
    public boolean exists() throws BackendException {
        try {
            return DirectoryLayer.getDefault().exists(db, PathUtil.from(rootDirectoryName)).get();
        } catch (InterruptedException e) {
            throw new PermanentBackendException(e);
        } catch (ExecutionException e) {
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    private static class TransactionBegin extends Exception {
        private static final long serialVersionUID = 1L;

        private TransactionBegin(String msg) { super(msg); }
    }

    private String determineRootDirectoryName(Configuration config) {
        if (!config.has(DIRECTORY) && (config.has(GRAPH_NAME))) {
            return config.get(GRAPH_NAME);
        } else {
            return config.get(DIRECTORY);
        }
    }
}
