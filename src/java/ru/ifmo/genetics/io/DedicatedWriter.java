package ru.ifmo.genetics.io;

public interface DedicatedWriter<T> {
    public void start();
    public void stopAndWaitForFinish() throws InterruptedException;
    public Sink<T> getLocalSink();
}
