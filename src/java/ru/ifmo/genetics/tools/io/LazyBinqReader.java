package ru.ifmo.genetics.tools.io;

import ru.ifmo.genetics.dna.DnaQ;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class LazyBinqReader {

    public static final int DEFAULT_BUFFER_SIZE = 1 << 23;

    private MultipleFilesByteArrayReader reader;
    ByteBuffer bb = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
    int size = 0;

    boolean eof = false;

    public LazyBinqReader(File[] files) throws EOFException, FileNotFoundException {
        reader = new MultipleFilesByteArrayReader(files);
    }

    public LazyBinqReader(File file) throws EOFException, FileNotFoundException {
        this(new File[]{file});
    }

    public LazyBinqReader(String[] files) throws EOFException, FileNotFoundException {
        reader = new MultipleFilesByteArrayReader(files);
    }

    public LazyBinqReader(String file) throws EOFException, FileNotFoundException {
        this(new String[]{file});
    }

    private void adjust() throws IOException {
        if (bb.position() == size) {
            bb.clear();
            size = reader.read(bb.array());
            if (size == -1) {
                eof = true;
                throw new EOFException();
            }
        }
    }

    private byte nextByte() throws IOException {
        adjust();
        return bb.get();
    }

    private byte[] nextByteArray(int len) throws IOException {
        byte[] ar = new byte[len];
        int b = 0;
        while (b < len) {
            adjust();
            int step = Math.min(len - b, size - bb.position());
            bb.get(ar, b, step);
            b += step;
        }
        return ar;
    }

    public DnaQ readDnaq() throws IOException {
        if (eof) {
            throw new EOFException();
        }
        int len = 0;
        for (int i = 0; i < 4; ++i) {
            int n = nextByte();
            if (n == -1) {
                --i;
                continue;
            }
            if (n < 0) {
                 n += 256;
            }
            len = (len << 8) | n;
        }
        return new DnaQ(nextByteArray(len));
    }

}
