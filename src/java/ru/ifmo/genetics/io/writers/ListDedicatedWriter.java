package ru.ifmo.genetics.io.writers;

import ru.ifmo.genetics.io.DedicatedWriter;
import ru.ifmo.genetics.io.Sink;

import java.util.List;

public class ListDedicatedWriter<T> implements DedicatedWriter<T> {
    public final List<T> list;

    public ListDedicatedWriter(List<T> list) {
        this.list = list;
    }

    @Override
    public void start() {
    }

    @Override
    public void stopAndWaitForFinish() throws InterruptedException {
    }

    @Override
    public Sink<T> getLocalSink() {
        return new MySink();
    }

    public class MySink implements Sink<T> {
        @Override
        public void put(T v) {
            list.add(v);
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }
}
