package com.experoinc.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.subspace.Subspace;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;

/**
 * @author Florian Grieskamp
 */
public class FoundationDBRangeQuery {

    public static final StaticBuffer.Factory<byte[]> ENTRY_FACTORY = (array, offset, limit) -> {
        final byte[] bArray = new byte[limit - offset];
        System.arraycopy(array, offset, bArray, 0, limit - offset);
        return bArray;
    };

    private KVQuery originalQuery;
    private byte[] startKey;
    private byte[] endKey;
    private int limit;

    public FoundationDBRangeQuery(Subspace db, KVQuery kvQuery) {
        originalQuery = kvQuery;
        startKey = db.pack(kvQuery.getStart().as(FoundationDBRangeQuery.ENTRY_FACTORY));
        endKey = db.pack(kvQuery.getEnd().as(FoundationDBRangeQuery.ENTRY_FACTORY));
        limit = kvQuery.getLimit();
    }

    public KVQuery asKVQuery() { return originalQuery; }

    public byte[] getStartKey() { return startKey; }

    public byte[] getEndKey() { return endKey; }

    public int getLimit() { return limit; }
}