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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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
        List<KeyValue> filteredResult = new ArrayList<>();

        final List<KeyValue> unfilteredResult = tx.getRange(new FoundationDBRangeQuery(db, query));
        if (unfilteredResult != null) {

            // only take KV pairs that match the KeySelector rules
            for (final KeyValue kv : unfilteredResult) {
                StaticBuffer key = getBuffer(db.unpack(kv.getKey()).getBytes(0));

                if (query.getKeySelector().include(key)) {
                    filteredResult.add(kv);
                }
            }
        }

        log.trace("db={}, op=getSlice, tx={}, resultcount={}", name, txh, filteredResult.size());
        return new FoundationDBRecordIterator(db, filteredResult);
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

        final Map<KVQuery, List<KeyValue>> unfilteredResultMap =
            tx.getMultiRange(queries.stream()
                                 .map(query -> new FoundationDBRangeQuery(db, query))
                                 .collect(Collectors.toList()));

        for (Entry<KVQuery, List<KeyValue>> currentUnfilteredResultMap : unfilteredResultMap.entrySet()) {
            KVQuery currentQuery = currentUnfilteredResultMap.getKey();
            List<KeyValue> currentUnfilteredResult = currentUnfilteredResultMap.getValue();

            if (currentUnfilteredResult != null) {
                // only take KV pairs that match the KeySelector rules
                for (final KeyValue kv : currentUnfilteredResult) {
                    StaticBuffer key = getBuffer(db.unpack(kv.getKey()).getBytes(0));
                    if (currentQuery.getKeySelector().include(key)) {
                        completeResultMap.get(currentQuery).add(kv);
                    }
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
