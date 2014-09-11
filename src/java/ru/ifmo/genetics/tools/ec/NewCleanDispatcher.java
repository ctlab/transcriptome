package ru.ifmo.genetics.tools.ec;

import it.unimi.dsi.fastutil.longs.Long2ByteMap;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import ru.ifmo.genetics.io.MultiFile2MemoryMap;

public class NewCleanDispatcher {

    final DataInputStream in;
    final int workRange;

    long kmersProcessed = 0;
    long lastKmersProcessed = 0;
    MultiFile2MemoryMap mf;

    long lastTimesSize = 0;

    long lastTime;

    public NewCleanDispatcher(DataInputStream in, int workRange, MultiFile2MemoryMap mf) {
        this.in = in;
        this.workRange = workRange;
        this.mf = mf;

        this.lastTime = System.currentTimeMillis();
    }

    public synchronized List<byte[]> getWorkRange(Long2ByteMap times) {
        List<byte[]> list = new ArrayList<byte[]>();
        while (true) {
            try {
                long kmer = in.readLong();
                int l = in.readInt() + 8;
                byte[] temp = new byte[l];
                for (int i = 0; i < 8; ++i) {
                    temp[i] = (byte)((kmer >>> (8 * (7 - i))) & 255);
                }
                int tl = 8;
                while (tl < l) {
                    tl += in.read(temp, tl, l - tl);
                }

                list.add(temp);
                if (list.size() == workRange) {
                    break;
                }

                ++kmersProcessed;
                /*
                if (kmersProcessed - lastKmersProcessed >= 1000) {
                    System.err.println(kmersProcessed + " kmers processed");
                    lastKmersProcessed = kmersProcessed;
                    mf.dump();
                    System.err.println("dumped");
                    System.err.println("it took " + (System.currentTimeMillis() - lastTime) / 1000 + " s");
                    lastTime = System.currentTimeMillis();
                }
                */
            } catch (EOFException e) {
                break;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        if (times.size() - lastTimesSize >= 100000) {
            System.err.println("timesSize = " + times.size());
            lastTimesSize = times.size();
            System.err.println("it took " + (System.currentTimeMillis() - lastTime) / 1000 + " s");
            lastTime = System.currentTimeMillis();
            System.err.println("reads skipped: " + Kmer2ReadIndexBuilder.uncorrected);
            try {
                mf.dump();
            } catch (IOException e) {
                System.err.println("ERROR: unable to dump");
            }
        }
        return list.isEmpty() ? null : list;
    }

}
