package ru.ifmo.genetics.structures.set;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

public class ArrayLong2LongHashMap {

    public Long2LongOpenHashMap[] hm;
    long mask;

    public ArrayLong2LongHashMap(int logMapsNumber) {
        int mapsNumber = 1 << logMapsNumber;
        mask = mapsNumber - 1;

        hm = new Long2LongOpenHashMap[mapsNumber];
        for (int i = 0; i < mapsNumber; ++i) {
            hm[i] = new Long2LongOpenHashMap();
        }
    }

    public long put(long key, long value) {
        int ind = (int)(key & mask);
        synchronized (hm[ind]) {
            return hm[ind].put(key, value);
        }
    }

    public long get(long key) {
        int ind = (int)(key & mask);
        return hm[ind].get(key);
    }

    public boolean containsKey(long key) {
        int ind = (int)(key & mask);
        return hm[ind].containsKey(key);
    }

    public long size() {
        long size = 0;
        for (Long2LongMap m : hm) {
            size += m.size();
        }
        return size;
    }

}
