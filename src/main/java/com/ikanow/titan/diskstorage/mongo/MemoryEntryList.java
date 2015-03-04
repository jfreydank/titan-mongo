package com.ikanow.titan.diskstorage.mongo;

import java.util.ArrayList;
import java.util.Iterator;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryList;

public class MemoryEntryList extends ArrayList<Entry> implements EntryList {

	private static final long serialVersionUID = 1L;

	public MemoryEntryList(int size) {
        super(size);
    }

    @Override
    public Iterator<Entry> reuseIterator() {
        return iterator();
    }

    @Override
    public int getByteSize() {
        int size = 48;
        for (Entry e : this) {
            size += 8 + 16 + 8 + 8 + e.length();
        }
        return size;
    }
}
