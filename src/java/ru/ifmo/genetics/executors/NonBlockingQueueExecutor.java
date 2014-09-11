package ru.ifmo.genetics.executors;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

public class NonBlockingQueueExecutor {
    final static int SLEEP_TIME = 50; // in millis

    final int nThreads;
    private Queue<Runnable> tasks;
    private CountDownLatch latch;
    private Thread[] workers;

    public NonBlockingQueueExecutor(int nThreads) {
        this.nThreads = nThreads;
        tasks = new ConcurrentLinkedQueue<Runnable>();
        latch = new CountDownLatch(nThreads);
        workers = new Thread[nThreads];

        for (int i = 0; i < nThreads; i++) {
            workers[i] = new Thread(new Worker());
            workers[i].start();
        }
    }

    public void addTask(Runnable task) throws InterruptedException {
        tasks.add(task);
    }

    /**
     * Initiates an orderly shutdown in which previously submitted tasks are
     * executed, but no new tasks will be accepted.
     */
    public void shutdownAndAwaitTermination() throws InterruptedException {
        for (int i = 0; i < nThreads; i++) {
            tasks.add(new EndTask());
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            System.err.println("Main thread interrupted");
            for (Thread worker : workers) {
                worker.interrupt();
            }
        }
    }

    private class EndTask implements Runnable {
        @Override
        public void run() {
        }
    }

    public class Worker implements Runnable {
        @Override
        public void run() {
            while (true) {
                Runnable task = tasks.poll();

                if (task == null) {
                    try {
                        Thread.sleep(SLEEP_TIME);
                    } catch (InterruptedException e) {
                        break;
                    }
                } else {
                    if (task.getClass() == EndTask.class) {
                        break;
                    }
                    task.run();
                }
            }
            latch.countDown();
        }
    }
}
