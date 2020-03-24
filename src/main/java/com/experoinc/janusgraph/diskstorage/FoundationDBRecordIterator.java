package com.experoinc.janusgraph.diskstorage;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.experoinc.janusgraph.diskstorage.foundationdb.FoundationDBKeyValueStore;
import java.util.Iterator;
import java.util.List;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.util.RecordIterator;

public class FoundationDBRecordIterator implements RecordIterator<KeyValueEntry> {
    private final DirectorySubspace ds;

    private final Iterator<KeyValue> entries;
    private final KeySelector selector;
    private KeyValueEntry nextEntry;

    public FoundationDBRecordIterator(final DirectorySubspace ds, final List<KeyValue> result, KeySelector selector) {
        this.ds = ds;
        this.entries = result.iterator();
        this.selector = selector;
        nextEntry = findNextEntry();
    }

    private KeyValueEntry findNextEntry() {
        while (entries.hasNext()) {
            KeyValue candidate;
            StaticBuffer key;
            do {
                candidate = entries.next();
                key =
                    FoundationDBKeyValueStore.getBuffer(ds.unpack(candidate.getKey()).getBytes(0));
            } while (!selector.include(key) && entries.hasNext());

            if (selector.include(key)) {
                return new KeyValueEntry(key,
                                         FoundationDBKeyValueStore.getBuffer(candidate.getValue()));
            }
        }

        return null;
    }

    @Override
    public boolean hasNext() {
        return nextEntry != null;
    }

    @Override
    public KeyValueEntry next() {
        KeyValueEntry returnValue = nextEntry;
        nextEntry = findNextEntry();
        return returnValue;
    }

    @Override
    public void close() {}

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}