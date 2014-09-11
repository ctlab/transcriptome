package ru.ifmo.genetics.tools.ec;

import it.unimi.dsi.fastutil.booleans.*;
import it.unimi.dsi.fastutil.bytes.*;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.*;
import ru.ifmo.genetics.dna.DnaQ;
import ru.ifmo.genetics.dna.Dna;
import ru.ifmo.genetics.dna.kmers.ShortKmer;
import ru.ifmo.genetics.io.MultiFile2MemoryMap;
import ru.ifmo.genetics.io.RandomAccessMultiFile;
import ru.ifmo.genetics.tools.io.LazyLongReader;
import ru.ifmo.genetics.utils.KmerUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class Kmer2ReadIndexBuilder {

    static long corrected = 0;
    static long uncorrected = 0;

    public static void main(String[] args) throws IOException {
        int threadsNumber = Integer.parseInt(args[0]);
        System.err.println("Using " + threadsNumber + " thread(s)");
        int len = Integer.parseInt(args[1]);
        int err = Integer.parseInt(args[2]);
        int i = 3;
        for (; (i < args.length) && !args[i].equals("--"); ++i);
        String[] kmerFiles = Arrays.copyOfRange(args, 3, i);
        int b = i + 1;
        for (i = b; (i < args.length) && !args[i].equals("--"); ++i);
        String[] readFiles = Arrays.copyOfRange(args, b, i);
        String indexFile = args[i + 1];
        long ct;
        /*
        LazyLongReader reader = new LazyLongReader(kmerFiles);
        Long2IntMap kmers = new Long2IntOpenHashMap();
        while (true) {
            try {
                kmers.put(reader.readLong(), 0);
            } catch (EOFException e) {
                break;
            }
        }
        i = 0;
        for (long l : kmers.keySet()) {
            kmers.put(l, i++);
        }
        System.err.println(kmers.size() + " kmers loaded");
        int totalKmers = kmers.size();

        ct = System.currentTimeMillis();
        Kmer2ReadIndexBuilderDispatcher dispatcher = new Kmer2ReadIndexBuilderDispatcher(readFiles, 1 << 18);
        Kmer2ReadIndexBuilderWorker[] workers = new Kmer2ReadIndexBuilderWorker[threadsNumber];
        CountDownLatch latch = new CountDownLatch(workers.length);
        for (i = 0; i < workers.length; ++i) {
            workers[i] = new Kmer2ReadIndexBuilderWorker(kmers, len, totalKmers, dispatcher, latch);
            new Thread(workers[i]).start();
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            System.err.println("interrupted");
            for (Kmer2ReadIndexBuilderWorker worker : workers) {
                worker.interrupt();
            }
            System.exit(1);
        }

        System.err.println("building done in " + (System.currentTimeMillis() - ct));

        ct = System.currentTimeMillis();
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexFile)));
        Iterator<Long> it = kmers.keySet().iterator();
        for (i = 0; i < totalKmers; ++i) {
            int size = 0;
            for (Kmer2ReadIndexBuilderWorker worker : workers) {
                if (worker.index[i] != null) {
                    size += worker.index[i].size();
//                    System.err.println("i = " + i + "; size = " + worker.index[i].size());
                }
            }
            long kmer = it.next();
            out.writeLong(kmer);
            out.writeInt(size);
            for (Kmer2ReadIndexBuilderWorker worker : workers) {
                if (worker.index[i] != null) {
                    out.write(worker.index[i].toByteArray());
                }
            }
        }
        for (i = 0; i < workers.length; ++i) {
            workers[i] = null;
        }
        out.close();
        System.err.println("dumping done in " + (System.currentTimeMillis() - ct));
        */

        DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(indexFile)));

        ct = System.currentTimeMillis();
        MultiFile2MemoryMap mf = new MultiFile2MemoryMap(readFiles);
        System.err.println("reads loading done in " + (System.currentTimeMillis() - ct));
        NewCleanDispatcher cleanDispatcher = new NewCleanDispatcher(in, 10000, mf);
        NewCleanWorker[] cleanWorkers = new NewCleanWorker[threadsNumber];
        CountDownLatch cleanLatch = new CountDownLatch(cleanWorkers.length);
//        RandomAccessMultiFile mf = new RandomAccessMultiFile(readFiles, "rw");
        Long2ByteMap times = new Long2ByteOpenHashMap();
        ct = System.currentTimeMillis();
        for (i = 0; i < cleanWorkers.length; ++i) {
            cleanWorkers[i] = new NewCleanWorker(cleanLatch, cleanDispatcher, mf, len, times, err);
            new Thread(cleanWorkers[i]).start();
        }
        try {
            cleanLatch.await();
        } catch (InterruptedException e) {
            for (NewCleanWorker worker : cleanWorkers) {
                worker.interrupt();
            }
            throw new RuntimeException(e);
        }

        System.err.println("processing done in " + (System.currentTimeMillis() - ct));
        ct = System.currentTimeMillis();
        mf.dump();
        System.err.println("dumping done in " + (System.currentTimeMillis() - ct));

    }

}
