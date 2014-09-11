package ru.ifmo.genetics.tools.ec;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongCollection;
import it.unimi.dsi.fastutil.longs.LongList;
import ru.ifmo.genetics.tools.io.LazyLongReader;
import ru.ifmo.genetics.tools.io.MultipleFilesByteArrayReader;
import ru.ifmo.genetics.utils.KmerUtils;
import ru.ifmo.genetics.utils.tool.Tool;

public class CleanDispatcher {
    LazyLongReader reader;
    int workRangeSize;
    long bad, total = 0;
    int LEN;
    int threadsNumber;

    public CleanDispatcher(LongCollection badKmers, int workRangeSize) {
        tasks = new ArrayList<List<Long>>();

        List<Long> task = new ArrayList<Long>(workRangeSize);
        for (long str : badKmers) {
            task.add(str);
            if (task.size() == workRangeSize) {
                tasks.add(task);
                task = new ArrayList<Long>(workRangeSize);
            }
        }

        if (task.size() != 0) {
            tasks.add(task);
        }

    }

    public CleanDispatcher(LazyLongReader reader, int workRangeSize, long bad, int LEN, int threadsNumber)
            throws FileNotFoundException, EOFException {
        this.reader = reader;
        this.workRangeSize = workRangeSize;
        this.bad = bad;
        this.threadsNumber = threadsNumber;

        this.LEN = LEN;

        Tool.createProgressBar(bad << 1);
    }

    private LongList readRange(LazyLongReader reader, int workRangeSize) throws IOException {
        LongList list = new LongArrayList(workRangeSize);
        while (list.size() < workRangeSize) {
            try {
                long fw, rc;
                synchronized (reader) {
                    fw = reader.readLong();
                }
                rc = KmerUtils.reverseComplement(fw, LEN);
                list.add(fw);
                list.add(rc);
            } catch (EOFException e) {
                break;
            }
        }
        return list;
    }

    List<List<Long>> tasks;

    public LongList getWorkRange() {
        LongList list;
        try {
            synchronized (reader) {
                list = readRange(reader, workRangeSize);
            }
            total += workRangeSize;
            Tool.updateProgressBar(Math.max(0, total - threadsNumber * workRangeSize));
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return list.isEmpty() ? null : list;
    }

}
