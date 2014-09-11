package ru.ifmo.genetics.structures.debriujn;

import org.apache.hadoop.io.Writable;
import ru.ifmo.genetics.dna.kmers.BigKmer;
import ru.ifmo.genetics.structures.set.BigLongsHashSet;
import ru.ifmo.genetics.structures.set.LongsHashSet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompactDeBruijnGraph extends AbstractDeBruijnGraph implements Writable, DeBruijnGraph {
    private BigLongsHashSet edges;

    public CompactDeBruijnGraph() {
        edges = new BigLongsHashSet();
    }

    public CompactDeBruijnGraph(int k, long memSize) {
        setK(k);
        edges = new BigLongsHashSet(memSize);
    }

    public void reset() {
        edges.reset();
    }

    @Override
    public boolean addEdge(BigKmer e) {
        return putEdge(e.biLongHashCode());
    }

    public boolean put(long eLongHashCode) {
        return edges.put(eLongHashCode);
    }

    @Override
    public boolean containsEdge(BigKmer e) {
        return edges.contains(e.biLongHashCode());
    }

    public long edgesSize() {
        return edges.size();
    }


    @Override
    public void write(DataOutput out) throws IOException {
        edges.write(out);
        out.writeInt(k);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        edges.readFields(in);
        k = in.readInt();
        setK(k);
    }

    public boolean putEdge(long kmerHash) {
        return edges.put(kmerHash);
    }
}

