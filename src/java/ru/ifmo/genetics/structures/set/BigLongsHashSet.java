package ru.ifmo.genetics.structures.set;

import it.unimi.dsi.fastutil.HashCommon;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.log4j.Logger;
import ru.ifmo.genetics.structures.arrays.BigBooleanArray;
import ru.ifmo.genetics.structures.arrays.BigLongArray;
import ru.ifmo.genetics.utils.NumUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class BigLongsHashSet implements LongsHashSet {
    private Logger logger = Logger.getLogger(BigLongsHashSet.class);
    BigLongArray ar;
    BigBooleanArray used;

    long capacity;
    long size = 0;
    
    static final int hashSizePowerOf2 = 16;
    static final int hashMask = (1 << hashSizePowerOf2) - 1;
    private boolean additionalHashing = true;
    private long logGranularity;
    private int granulaMask;
    private int[] m;
    private int granulasNumber;

    public BigLongsHashSet() {
        ar = new BigLongArray();
        used = new BigBooleanArray();
    }
    
    public BigLongsHashSet(long minCapacity, double loadFactor) {
        minCapacity = (long) (minCapacity / loadFactor);
        minCapacity = Math.max(1, minCapacity);

        int cnt = (int) Math.ceil(minCapacity / (double) BigLongArray.smallCapacity);
        cnt = (int) NumUtils.highestBitsUpperBound(cnt, 3);

        init((long) BigLongArray.smallCapacity * cnt);

        ar = new BigLongArray(capacity);
        used = new BigBooleanArray(capacity);

    }

    private void init(long capacity) {
        this.capacity = capacity;
        logGranularity = Long.numberOfTrailingZeros(Long.lowestOneBit(capacity));
        granulaMask = (1 << logGranularity) - 1;

        granulasNumber = Math.max((int)(capacity >>> logGranularity), 1);

        m = new int[1 << hashSizePowerOf2];
        for (int i = 0; i < m.length; i++) {
            m[i] = i % granulasNumber;
        }
    }

    /**
     * @param memoryUsage in bytes
     */
    public BigLongsHashSet(long memoryUsage) {
        this(memoryUsage / 8, 1);
    }
    
    @Override
    public boolean contains(long v) {
        long i = getPossiblePosition(v);
        return used.get(i);
    }

    public boolean putAt(long i, long v) {
        if (used.get(i)) {
            return false;
        }
        size++;
        used.set(i, true);
        ar.set(i, v);
        return true;
    }

    @Override
    public boolean put(long v) {
        long i = getPossiblePosition(v);
        return putAt(i, v);
    }
    
    @Override
    public long size() {
        return size;
    }

    @Override
    public long capacity() {
        return capacity;
    }

    public void reset() {
        reset(capacity);
    }

    @Override
    public void reset(long newCapacity) {
        size = 0;
        if (newCapacity != capacity()) {
            init(newCapacity);
            return;
        }

        for (long i = 0; i < capacity; i++) {
            used.set(i, false);
        }
    }

    /**
     * Finds position where element v is stored of where it should be stored
     * @param v
     * @return
     */
    public long getPossiblePosition(long v) {
        long h = hash(v);

        long a = h >>> logGranularity;

        int i = (int) (a ^ (a >>> 33));
        i = i ^ (i >>> 16);
        i = i & hashMask;

        long pos = (m[i] << logGranularity) + (h & granulaMask);

        long begPos = pos;

        while (used.get(pos) && ar.get(pos) != v) {
            pos++;
            if (pos == capacity) {
                pos = 0;
            }
        }
        return pos;
    }

    @Override
    public long getPosition(long v) {
        long i = getPossiblePosition(v);
        return used.get(i) ? i : - 1;
    }

    private long hash(long v) {
        if (!additionalHashing)
            return v;
        return HashCommon.murmurHash3(v);
    }

    @Override
    public long elementAt(long i) {
        return ar.get(i);
    }
    
    // Test
    public static void main(String[] args) {
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(size);
        out.writeLong(capacity);

        ar.write(out);
        used.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        size = in.readLong();
        capacity = in.readLong();

        init(capacity);

        ar.readFields(in);
        used.readFields(in);
    }

    public boolean containsAt(long i) {
        return used.get(i);
    }

    @Override
    public java.util.Iterator<MutableLong> iterator() {
        return new Iterator();
    }

    protected class Iterator implements java.util.Iterator<MutableLong> {
        private long index = 0;
        private MutableLong value = new MutableLong();


        @Override
        public boolean hasNext() {
            while (index < ar.size()) {
                if (!used.get(index)) {
                    index++;
                    continue;
                }
                break;
            }
            return index < ar.size();
        }

        @Override
        public MutableLong next() {
            if (hasNext()){
                value.setValue(ar.get(index));
                index++;
                return value;
            }
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
