package ru.ifmo.genetics.io.readers;

import ru.ifmo.genetics.dna.DnaQ;
import ru.ifmo.genetics.io.sources.NamedSource;

import java.io.File;
import java.io.IOException;

public class NamedBinqSource extends BinqReader implements NamedSource<DnaQ> {
    String name;

    public NamedBinqSource(String file) throws IOException {
        this (new File(file));
    }

    public NamedBinqSource(File file) throws IOException {
        super(file);
        name = file.getAbsolutePath();
    }

    @Override
    public String name() {
        return name;
    }
}
