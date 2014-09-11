package ru.ifmo.genetics.dna.kmers;

import ru.ifmo.genetics.dna.Dna;
import ru.ifmo.genetics.dna.DnaView;
import ru.ifmo.genetics.dna.LightDna;
import ru.ifmo.genetics.utils.KmerUtils;

import java.util.Iterator;

public class ShortKmer implements Kmer {
    private final int length;
    private final long mask;

    private long fwKmer;
    private long rcKmer;

    public ShortKmer(long kmer, int length) {
        this.length = length;
        mask = (1L << (2 * length)) - 1;
        this.fwKmer = kmer;
        this.rcKmer = KmerUtils.reverseComplement(kmer, length);
    }

    public ShortKmer(LightDna d) {
        length = d.length();
        mask = (1L << (2 * length)) - 1;

        fwKmer = KmerUtils.toLong(d, 0, d.length());
        rcKmer = KmerUtils.reverseComplement(fwKmer, length);
    }

    public ShortKmer(CharSequence s) {
        this(new Dna(s));
    }

    public long fwKmer() {
        return fwKmer;
    }

    public long rcKmer() {
        return rcKmer;
    }

    public long toLong() {
        return Math.min(fwKmer, rcKmer);
    }

    @Override
    public byte nucAt(int ind) {
        return (byte)((fwKmer >> (2 * (length - 1 - ind))) & 3);
    }

    @Override
    public int length() {
        return length;
    }

    public void shiftRight(byte nuc) {
        fwKmer = ((fwKmer << 2) | nuc) & mask;
        rcKmer = (rcKmer >> 2) | ((3L - nuc) << (2 * length - 2));
    }

    public void appendRight(byte nuc) {
        fwKmer = ((fwKmer << 2) | nuc);
        rcKmer = rcKmer | ((3L - nuc) << (2 * length));
    }

    public void appendLeft(byte nuc) {
        fwKmer = fwKmer | (((long)nuc) << (2 * length));
        rcKmer = (rcKmer << 2) | (3 - nuc);
    }

    public void shiftLeft(byte nuc) {
        fwKmer = (fwKmer >> 2) | (((long)nuc) << (2 * length - 2));
        rcKmer = ((rcKmer << 2) | (3 - nuc)) & mask;
    }

    private void updateAt(int i, byte oldNuc, byte newNuc) {
        fwKmer ^= (long)(oldNuc ^ newNuc) << (2 * length - 2 * i - 2);
        rcKmer ^= (long)(oldNuc ^ newNuc) << (2 * i);

    }
    public void updateAt(int i, byte nuc) {
        updateAt(i, nucAt(i), nuc);
    }

    public static Iterable<ShortKmer> kmersOf(LightDna dna, int k) {
        return new KmerIterable(dna, k);
    }

    private static class KmerIterable implements Iterable<ShortKmer> {
        final LightDna dna;
        final int k;

        private KmerIterable(LightDna dna, int k) {
            this.dna = dna;
            this.k = k;
        }

        @Override
        public Iterator<ShortKmer> iterator() {
            return new KmerIterator();
        }

        private class KmerIterator implements Iterator<ShortKmer> {
            int i = k - 1;
            ShortKmer kmer = null;

            @Override
            public boolean hasNext() {
                return i < dna.length();
            }

            @Override
            public ShortKmer next() {
                if (kmer == null) {
                    kmer = new ShortKmer(new DnaView(dna, 0, k));
                } else {
                    kmer.shiftRight(dna.nucAt(i));
                }
                ++i;
                return kmer;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        }
    }

}
