package ru.ifmo.genetics.utils.iterators;

import ru.ifmo.genetics.dna.Dna;
import ru.ifmo.genetics.dna.DnaQ;
import ru.ifmo.genetics.io.sources.NamedSource;

public class DnaQReaderFromDnaSource implements NamedSource<DnaQ> {
    private NamedSource<Dna> dnaSource;
    private int phred;

    public DnaQReaderFromDnaSource(NamedSource<Dna> dnaSource) {
        this(dnaSource, 0);
    }

    public DnaQReaderFromDnaSource(NamedSource<Dna> dnaSource, int phred) {
        this.dnaSource = dnaSource;
        this.phred = phred;
    }

    @Override
    public ProgressableIterator<DnaQ> iterator() {
        return new DnaQIteratorFromDnaIterator(dnaSource.iterator(), phred);
    }

    @Override
    public String name() {
        return dnaSource.name();
    }
}
