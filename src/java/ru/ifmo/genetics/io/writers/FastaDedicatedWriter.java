package ru.ifmo.genetics.io.writers;

import org.apache.log4j.Logger;
import ru.ifmo.genetics.dna.Dna;
import ru.ifmo.genetics.dna.LightDna;
import ru.ifmo.genetics.io.DedicatedWriter;
import ru.ifmo.genetics.io.IOUtils;
import ru.ifmo.genetics.io.Sink;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class FastaDedicatedWriter implements DedicatedWriter<LightDna> {
    private Logger log = Logger.getLogger("Writer");

    private File file;
    private Queue<List<Dna>> queue;
//    private boolean printNs;

    private Thread writingThread;

    public FastaDedicatedWriter(File file) {
        this.file = file;
//        this.printNs = printNs;
        queue = new ConcurrentLinkedQueue<List<Dna>>();
        writingThread = new Thread(new WriterTask());
    }

    @Override
    public void start() {
        writingThread.start();
    }

    @Override
    public void stopAndWaitForFinish() throws InterruptedException {
        List<Dna> endList = new ArrayList<Dna>(1);
        endList.add(null);

        queue.add(endList);
        writingThread.join();
    }

    @Override
    public Sink<LightDna> getLocalSink() {
        return new DnaSink(queue);
    }

    private class WriterTask implements Runnable {
        @Override
        public void run() {
            long nextId = 0;

            long tasksWritten = 0;
            boolean firstTime = true;
            while (true) {
                List<Dna> dnas;
                dnas = queue.poll();
                if (dnas == null) {
                    try {
                        Thread.sleep(123);
                    } catch (InterruptedException e) {
                        System.err.println("writing thread interrupted");
                        break;
                    }
                    continue;
                }

                if ((dnas.size() == 1) && (dnas.get(0) == null)) {
                    break;
                }

                try {
//                    IOUtils.dnaqs2FastaFile(dnas, file, !firstTime, c, printNs);
                    IOUtils.dnas2FastaFile(dnas, file, !firstTime, nextId);
                } catch (IOException e) {
                    log.error("Error while writing to file ");
                    e.printStackTrace(System.err);
                }
                nextId += dnas.size();

                firstTime = false;
                ++tasksWritten;
            }
            log.info(nextId + " sequences written");
        }
    }

    private static class DnaSink implements Sink<LightDna> {
        private final Queue<List<Dna>> queue;
        private List<Dna> buffer;

        private DnaSink(Queue<List<Dna>> queue) {
            this.buffer = new ArrayList<Dna>();
            this.queue = queue;
        }


        @Override
        public void put(LightDna v) {
            buffer.add(new Dna(v));
        }

        @Override
        public void flush() {
            queue.add(buffer);
            buffer = new ArrayList<Dna>();
        }

        @Override
        public void close() {
            flush();
        }
    }
}
