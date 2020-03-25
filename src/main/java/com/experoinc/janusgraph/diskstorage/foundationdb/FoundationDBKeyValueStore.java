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

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.experoinc.janusgraph.diskstorage.FoundationDBRecordIterator;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
public class FoundationDBKeyValueStore implements OrderedKeyValueStore {

    private static final Logger log = LoggerFactory.getLogger(FoundationDBKeyValueStore.class);

    private final DirectorySubspace db;
    private final String name;
    private final FoundationDBStoreManager manager;
    private boolean isOpen;

    public FoundationDBKeyValueStore(String n, DirectorySubspace data, FoundationDBStoreManager m) {
        db = data;
        name = n;
        manager = m;
        isOpen = true;
    }

    @Override
    public String getName() {
        return name;
    }

    private static FoundationDBTx getTransaction(StoreTransaction txh) {
        Preconditions.checkArgument(txh != null);
        return (FoundationDBTx) txh;
    }

    @Override
    public synchronized void close() throws BackendException {
        if (isOpen) {
            manager.removeDatabase(this);
        }

        isOpen = false;
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws BackendException {
        FoundationDBTx tx = getTransaction(txh);
        byte[] databaseKey = db.pack(key.as(FoundationDBRangeQuery.ENTRY_FACTORY));
        log.trace("db={}, op=get, tx={}", name, txh);
        final byte[] entry = tx.get(databaseKey);
        if (entry != null) {
            return getBuffer(entry);
        } else {
            return null;
        }
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws BackendException {
        return get(key, txh) != null;
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh)
        throws BackendException {
        if (getTransaction(txh) == null) {
            log.warn("Attempt to acquire lock with transactions disabled");
        } // else we need no locking
    }

    @Override
    public RecordIterator<KeyValueEntry> getSlice(KVQuery query, StoreTransaction txh)
        throws BackendException {
        log.trace("beginning db={}, op=getSlice, tx={}", name, txh);
        final FoundationDBTx tx = getTransaction(txh);

        final List<KeyValue> completeResult = new ArrayList<>();
        final FoundationDBRangeQuery fdbQuery = new FoundationDBRangeQuery(db, query);

        // the query needs to be repeated if applying the KeySelector drops the result size
        // below the query limit
        while (completeResult.size() < query.getLimit()) {
            final List<KeyValue> partialResult = tx.getRange(fdbQuery);
            if (partialResult != null && partialResult.size() > 0) {
                int maximumCompleteResultSize = completeResult.size() + partialResult.size();
                KeyValue lastFoundKV = partialResult.get(partialResult.size() - 1);

                // only take KV pairs that match the KeySelector rules
                for (final KeyValue kv : partialResult) {
                    StaticBuffer key = getBuffer(db.unpack(kv.getKey()).getBytes(0));
                    if (query.getKeySelector().include(key)) {
                        completeResult.add(kv);
                    }
                }

                if (maximumCompleteResultSize < query.getLimit()) {
                    // further searching will not yield any more results than already
                    // accumulated
                    break;
                }

                // start the next range at read the end of the current result
                fdbQuery.setStartKeySelector(KeySelector.firstGreaterThan(lastFoundKV.getKey()));
            } else {
                // no more entries were found, so no reason to search any further
                break;
            }
        }

        log.trace("db={}, op=getSlice, tx={}, resultcount={}", name, txh, completeResult.size());
        return new FoundationDBRecordIterator(db, completeResult);
    }

    @Override
    public Map<KVQuery, RecordIterator<KeyValueEntry>> getSlices(List<KVQuery> queries,
                                                                 StoreTransaction txh)
        throws BackendException {
        log.trace("beginning db={}, op=getSlice, tx={}", name, txh);
        FoundationDBTx tx = getTransaction(txh);

        final Map<KVQuery, List<KeyValue>> completeResultMap = new ConcurrentHashMap<>();
        final Map<KVQuery, FoundationDBRangeQuery> fdbQueries = new ConcurrentHashMap<>();

        for (KVQuery q : queries) {
            completeResultMap.put(q, new ArrayList<>());
            fdbQueries.put(q, new FoundationDBRangeQuery(db, q));
        }

        // fdbQueries contains all queries that are not finished yet with respect to their limit
        while (fdbQueries.size() > 0) {
            final Map<KVQuery, List<KeyValue>> partialResultMap =
                tx.getMultiRange(fdbQueries.values());

            // for each query, check if it is complete after adding the newly obtained results
            for (Entry<KVQuery, List<KeyValue>> currentPartialResultMap :
                 partialResultMap.entrySet()) {
                KVQuery currentQuery = currentPartialResultMap.getKey();
                List<KeyValue> currentPartialResult = currentPartialResultMap.getValue();

                if (currentPartialResult != null && currentPartialResult.size() > 0) {
                    int maximumCompleteResultSize =
                        completeResultMap.get(currentQuery).size() + currentPartialResult.size();
                    KeyValue lastFoundKV =
                        currentPartialResult.get(currentPartialResult.size() - 1);

                    // only take KV pairs that match the KeySelector rules
                    for (final KeyValue kv : currentPartialResult) {
                        StaticBuffer key = getBuffer(db.unpack(kv.getKey()).getBytes(0));
                        if (currentQuery.getKeySelector().include(key)) {
                            completeResultMap.get(currentQuery).add(kv);
                        }
                    }

                    if (maximumCompleteResultSize < currentQuery.getLimit()) {
                        // further searching will not yield any more results than already
                        // accumulated
                        fdbQueries.remove(currentQuery);
                    } else {
                        // start the next range at read the end of the current result
                        fdbQueries.get(currentQuery)
                            .setStartKeySelector(
                                KeySelector.firstGreaterThan(lastFoundKV.getKey()));
                    }
                } else {
                    // no more entries were found, so no reason to search any further
                    fdbQueries.remove(currentQuery);
                }
            }
        }

        final Map<KVQuery, RecordIterator<KeyValueEntry>> iteratorMap = new HashMap<>();
        for (Entry<KVQuery, List<KeyValue>> kv : completeResultMap.entrySet()) {
            iteratorMap.put(kv.getKey(), new FoundationDBRecordIterator(db, kv.getValue()));
        }
        return iteratorMap;
    }

    @Override
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh)
        throws BackendException {
        FoundationDBTx tx = getTransaction(txh);
        log.trace("db={}, op=insert, tx={}", name, txh);
        tx.set(db.pack(key.as(FoundationDBRangeQuery.ENTRY_FACTORY)),
               value.as(FoundationDBRangeQuery.ENTRY_FACTORY));
    }

    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) throws BackendException {
        log.trace("Deletion");
        FoundationDBTx tx = getTransaction(txh);
        log.trace("db={}, op=delete, tx={}", name, txh);
        tx.clear(db.pack(key.as(FoundationDBRangeQuery.ENTRY_FACTORY)));
    }

    public static StaticBuffer getBuffer(byte[] entry) { return new StaticArrayBuffer(entry); }
}
