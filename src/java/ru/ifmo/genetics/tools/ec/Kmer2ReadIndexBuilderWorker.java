package ru.ifmo.genetics.tools.ec;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.bytes.ByteList;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import ru.ifmo.genetics.dna.DnaQ;
import ru.ifmo.genetics.dna.kmers.ShortKmer;
import ru.ifmo.genetics.utils.KmerUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class Kmer2ReadIndexBuilderWorker implements Runnable {

    private boolean interrupted = false;
    private final Long2IntMap kmers;
    final int len;
    final Kmer2ReadIndexBuilderDispatcher dispatcher;
    final ByteList[] index;

    final CountDownLatch latch;

    final List<DnaQ> dl = new ArrayList<DnaQ>();
    final LongList pl = new LongArrayList();

    public Kmer2ReadIndexBuilderWorker(Long2IntMap kmers, int len, int totalKmers,
                                       Kmer2ReadIndexBuilderDispatcher dispatcher, CountDownLatch latch) {
        this.kmers = kmers;
        this.len = len;
        this.dispatcher = dispatcher;
        index = new ByteList[totalKmers];
        this.latch = latch;
    }

    public void interrupt() {
        interrupted = true;
    }

    @Override
    public void run() {
        while (!interrupted) {
            dispatcher.getWorkRange(dl, pl);
            if (dl.isEmpty()) {
                break;
            }
            process(dl, pl);
        }
        latch.countDown();
    }

    private void process(List<DnaQ> dl, LongList pl) {
        for (int i = 0; i < dl.size(); ++i) {
            DnaQ dnaq = dl.get(i);
            long pos = pl.get(i) - dnaq.length - 4;
            int j = -1;
            for (ShortKmer kmer : ShortKmer.kmersOf(dnaq, len)) {
                long l = kmer.toLong();
                ++j;
                if (!kmers.containsKey(l)) {
                    continue;
                }
                add(l, pos);
            }
        }
    }

    private void add(long kmer, long pos) {
        int ind = kmers.get(kmer);
        if (index[ind] == null) {
            index[ind] = new ByteArrayList(8);
        }
        for (int i = 0; i < 8; ++i) {
            index[ind].add((byte)((pos >>> (8 * (7 - i))) & 255));
        }
    }

}
