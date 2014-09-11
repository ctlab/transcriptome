package ru.ifmo.genetics.structures.set;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.io.Writable;


public interface LongsHashSet extends Writable, Iterable<MutableLong> {
    public boolean contains(long v);
    public boolean containsAt(long i);

    public boolean put(long v);
    
    public long getPosition(long v);
    public long elementAt(long i);
    
    public long size();
    public long capacity();

    public void reset(long newCapacity);
}
