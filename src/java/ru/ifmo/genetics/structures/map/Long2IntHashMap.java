package ru.ifmo.genetics.structures.map;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.io.Writable;

public interface Long2IntHashMap extends Writable, Iterable<MutableLong> {
    public boolean contains(long key);
    public int put(long key, int value);
    public int get(long key);

    public long getPosition(long key);
    public long keyAt(long i);
    public int valueAt(long i);
    public boolean containsAt(long i);

    public long size();
    public long capacity();

    public void reset();
}
