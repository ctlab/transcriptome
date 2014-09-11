package ru.ifmo.genetics.tools.olc.optimizer;

import ru.ifmo.genetics.dna.Dna;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class RemovingCRTaskContext {

    public final int[] readLen;

    public final boolean[] removingRead;
    public final boolean[] readRemoved; // already (before Removing CR)

    public final AtomicInteger[] overlapsCount;


    public RemovingCRTaskContext(ArrayList<Dna> reads) {
        int readsNumber = reads.size();
        readLen = new int[readsNumber];
        removingRead = new boolean[readsNumber];
        readRemoved = new boolean[readsNumber];
        overlapsCount = new AtomicInteger[readsNumber];

        for (int i = 0; i < readsNumber; i++) {
            readLen[i] = reads.get(i).length();
            overlapsCount[i] = new AtomicInteger(0);
        }
    }
}
