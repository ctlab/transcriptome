package ru.ifmo.genetics.io.writers;

import ru.ifmo.genetics.io.Sink;

public class NullSink<T> implements Sink<T> {
    @Override
    public void put(T v) {
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
    }
}
