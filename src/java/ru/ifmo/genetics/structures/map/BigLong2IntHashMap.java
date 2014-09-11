package ru.ifmo.genetics.structures.map;

import org.apache.commons.lang.mutable.MutableLong;
import ru.ifmo.genetics.structures.arrays.BigIntegerArray;
import ru.ifmo.genetics.structures.set.BigLongsHashSet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BigLong2IntHashMap implements Long2IntHashMap {
    private BigLongsHashSet keys;
    private BigIntegerArray values;

    public BigLong2IntHashMap() {
        keys = new BigLongsHashSet();
        values = new BigIntegerArray();
    }

    public BigLong2IntHashMap(long minCapacity, double loadFactor) {
        keys = new BigLongsHashSet(minCapacity, loadFactor);
        values = new BigIntegerArray(keys.capacity());
    }

    /**
     * @param memoryUsageBytes memory to be used for map
     */
    public BigLong2IntHashMap(long memoryUsageBytes) {
        this(memoryUsageBytes / 12, 1);
    }

    @Override
    public boolean contains(long v) {
        return keys.contains(v);
    }

    public boolean containsAt(long i) {
        return keys.containsAt(i);
    }

    @Override
    public long keyAt(long i) {
        assert 0 <= i && i < values.size();
        return keys.elementAt(i);
    }

    @Override
    public int valueAt(long i) {
        assert i < values.size();
        return i < 0 ? 0 : values.get(i);
    }

    @Override
    public int get(long key) {
        long i = getPosition(key);
        return valueAt(i);
    }

    @Override
    public int put(long key, int value) {
        long i = keys.getPossiblePosition(key);
        keys.putAt(i, key);
        return values.set(i, value);
    }


    @Override
    public long size() {
        return keys.size();
    }

    @Override
    public long capacity() {
        return keys.capacity();
    }

    @Override
    public void reset() {
        keys.reset(keys.size());
    }

    @Override
    public long getPosition(long v) {
        return keys.getPosition(v);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        keys.write(out);
        values.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        keys.readFields(in);
        values.readFields(in);
    }

    @Override
    public java.util.Iterator<MutableLong> iterator() {
        return keys.iterator();
    }

}


