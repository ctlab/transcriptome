package ru.ifmo.genetics.io.writers;

import ru.ifmo.genetics.executors.Latch;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class DedicatedWriter implements Runnable {
    private final static int BUFFERS_NUMBER = 100;
    private final static int BUFFERS_SIZE = 1 << 21;

    private BlockingQueue<ByteBuffer> freeBuffers = new ArrayBlockingQueue<ByteBuffer>(BUFFERS_NUMBER);
    private BlockingQueue<ByteBuffer> buffersToWrite = new ArrayBlockingQueue<ByteBuffer>(BUFFERS_NUMBER);

    private OutputStream out;

//    private ConcurrentLinkedQueue<List<String>> queue;
//    private AtomicInteger size;

    private Latch writingThreads;
    private volatile boolean finished;


    public DedicatedWriter(String fileName) throws FileNotFoundException {
        this(new FileOutputStream(fileName));
    }

    public DedicatedWriter(OutputStream out) {
        this.out = out;
//        queue = new ConcurrentLinkedQueue<List<String>>();
//        size = new AtomicInteger(0);
        writingThreads = new Latch();
        finished = false;
        for (int i = 0; i < BUFFERS_NUMBER; ++i) {
            freeBuffers.add(ByteBuffer.allocate(BUFFERS_SIZE));
        }
    }


    public ByteBuffer getBuffer() throws InterruptedException {
        ByteBuffer res = freeBuffers.take();
        res.clear();
        return res;
    }

    public void returnBuffer(ByteBuffer buffer) {
        buffer.flip();
        buffersToWrite.add(buffer);
    }


    /*
    public int add(List<String> listToWrite) {
        assert !finished;

        queue.add(listToWrite);
        increaseAndNotify();
        return queue.size();
    }

    private void increaseAndNotify() {
        int prevSize = size.getAndIncrement();
        if (prevSize == 0) {
            synchronized (queue) {
                queue.notify();
            }
        }
    }
    */

    /**
     * Call this method when no thread will add a list to write.
     * This call blocks until all list will be written.
     */
    public void close() {
        finished = true;
//        increaseAndNotify();

        try {
            writingThreads.await();
        } catch (InterruptedException e) {
            System.err.println("Method await was interrupted!");
        }
        try {
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        writingThreads.increase();
        try {
            while ((freeBuffers.size() != BUFFERS_NUMBER) || !finished) {
                ByteBuffer buffer = buffersToWrite.poll(1, TimeUnit.SECONDS);
                if (buffer == null) {
                    continue;
                }

                out.write(buffer.array(), buffer.position(), buffer.limit());
                freeBuffers.add(buffer);
            }
        } catch (InterruptedException e) {
            System.err.println("Writing thread was interrupted!");
        } catch (IOException e) {
            System.err.println("IOException!");
            e.printStackTrace();
        } finally {
            writingThreads.decrease();
        }
    }
}
