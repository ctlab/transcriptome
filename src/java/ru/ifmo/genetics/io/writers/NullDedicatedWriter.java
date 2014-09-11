package ru.ifmo.genetics.io.writers;

import ru.ifmo.genetics.io.DedicatedWriter;
import ru.ifmo.genetics.io.Sink;

public class NullDedicatedWriter<T> implements DedicatedWriter<T> {
    @Override
    public void start() {
    }

    @Override
    public void stopAndWaitForFinish() throws InterruptedException {
    }

    @Override
    public Sink<T> getLocalSink() {
        return new NullSink<T>();
    }
}
