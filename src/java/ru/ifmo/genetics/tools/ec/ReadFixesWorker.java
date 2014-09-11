package ru.ifmo.genetics.tools.ec;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import ru.ifmo.genetics.structures.set.ArrayLong2LongHashMap;
import ru.ifmo.genetics.tools.io.LazyLongReader;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ReadFixesWorker implements Runnable {

    ReadFixesDispatcher dispatcher;
    ArrayLong2LongHashMap allFixes;
    boolean interrupted;
    CountDownLatch latch;

    public ReadFixesWorker(ReadFixesDispatcher dispatcher, ArrayLong2LongHashMap allFixes, CountDownLatch latch) {
        this.dispatcher = dispatcher;
        this.allFixes = allFixes;
        this.latch = latch;
    }

    public void interrupt() {
        interrupted = true;
    }

    public void run() {
        LongList kmers = new LongArrayList();
        LongList fixes = new LongArrayList();
        while (!interrupted) {
            dispatcher.getWorkRange(kmers, fixes);
            if (kmers.isEmpty()) {
                break;
            }
            for (int i = 0; i < kmers.size(); ++i) {
                allFixes.put(kmers.getLong(i), fixes.getLong(i));
            }
        }
        latch.countDown();;
    }
}
