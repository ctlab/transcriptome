package ru.ifmo.genetics.io.readers;

import ru.ifmo.genetics.dna.Dna;
import ru.ifmo.genetics.dna.DnaQ;
import ru.ifmo.genetics.io.sources.NamedSource;
import ru.ifmo.genetics.io.sources.Source;
import ru.ifmo.genetics.utils.iterators.ProgressableIterator;

import java.io.IOException;

public class FastaReaderFromXQSource implements NamedSource<Dna> {
    private final NamedSource<DnaQ> source;

    public FastaReaderFromXQSource(NamedSource<DnaQ> source) throws IOException {
        this.source = source;
    }


    @Override
    public ProgressableIterator<Dna> iterator() {
        try {
            return new MyIterator(source);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String name() {
        return source.name();
    }


    class MyIterator implements ProgressableIterator<Dna> {
        ProgressableIterator<DnaQ> iterator;

        public MyIterator(Source<DnaQ> source) throws IOException {
            iterator = source.iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Dna next() {
            return new Dna(iterator.next());
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public double progress() {
            return iterator.progress();
        }
    }
}
