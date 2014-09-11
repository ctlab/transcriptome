package ru.ifmo.genetics.tools.rf;

import ru.ifmo.genetics.dna.DnaQ;
import ru.ifmo.genetics.dna.DnaQView;
import ru.ifmo.genetics.dna.IDnaQ;
import ru.ifmo.genetics.io.IOUtils;
import ru.ifmo.genetics.io.readers.BinqReader;
import ru.ifmo.genetics.utils.iterators.UniZippingIterator;
import ru.ifmo.genetics.utils.pairs.UniPair;
import ru.ifmo.genetics.utils.tool.ExecutionFailedException;
import ru.ifmo.genetics.utils.tool.Parameter;
import ru.ifmo.genetics.utils.tool.Tool;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.FileMVParameterBuilder;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.FileParameterBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class ReadsMerger extends Tool {
    public static final String NAME = "reads-merger";
    public static final String DESCRIPTION = "merges not truncated original reads and truncated corrected reads";


    public Parameter<File[]> originalReads = addParameter(new FileMVParameterBuilder("original-reads")
            .mandatory()
            .withDescription("original not truncated reads (in binq)")
            .create());

    public Parameter<File[]> correctedReads = addParameter(new FileMVParameterBuilder("corrected-reads")
            .mandatory()
            .withDescription("corrected truncated reads (in binq)")
            .create());

    public final Parameter<File> outputDir = addParameter(new FileParameterBuilder("output-dir")
            .optional()
            .withShortOpt("o")
            .withDescription("directory to output built quasicontigs")
            .withDefaultValue(workDir.append("quasicontigs"))
            .create());


    @Override
    protected void runImpl() throws ExecutionFailedException {
        outputDir.get().mkdir();

        Iterator<File> originalReadsIterator= Arrays.asList(originalReads.get()).iterator();
        Iterator<File> correctedReadsIterator= Arrays.asList(correctedReads.get()).iterator();

        for (Iterator<UniPair<File>> it = new UniZippingIterator<File>(originalReadsIterator, correctedReadsIterator); it.hasNext(); ) {
            UniPair<File> cur = it.next();
            BinqReader initial = null;
            try {
                initial = new BinqReader(cur.first);
            } catch (IOException e) {
                throw new ExecutionFailedException("Can't create library from from file " + cur.first, e);
            }

            BinqReader fixed = null;
            try {
                fixed = new BinqReader(cur.second);
            } catch (IOException e) {
                throw new ExecutionFailedException("Can't create library from from file " + cur.second, e);
            }

            String fn = (new File(initial.name())).getName();
//            System.err.println("fn = " + fn);

            try {
                IOUtils.dnaQs2BinqFile(
                        new MergeIterator(
                                fixed.iterator(),
                                initial.iterator()),
                        outputDir.get() + File.separator + initial.name() + ".binq", false);
            } catch (IOException e) {
                throw new ExecutionFailedException(e);
            }
        }
    }

    private static class MergeIterator implements Iterator<DnaQ> {
        private Iterator<DnaQ> fIt;
        private Iterator<DnaQ> sIt;

        public MergeIterator(Iterator<DnaQ> fIt, Iterator<DnaQ> sIt) {
            this.fIt = fIt;
            this.sIt = sIt;
        }

        public boolean hasNext() {
            return fIt.hasNext() && sIt.hasNext();
        }

        public DnaQ next() {
            DnaQ first = fIt.next();
            IDnaQ second = sIt.next();
            return new DnaQ(first, new DnaQView(second, first.length(), second.length()));
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    protected void clean() throws ExecutionFailedException {
    }

    public static void main(String[] args) throws Exception {
        new ReadsMerger().mainImpl(args);
    }

    public ReadsMerger() {
        super(NAME, DESCRIPTION);
    }
}
