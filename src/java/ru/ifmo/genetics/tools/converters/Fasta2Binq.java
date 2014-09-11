package ru.ifmo.genetics.tools.converters;

import ru.ifmo.genetics.dna.Dna;
import ru.ifmo.genetics.dna.DnaQ;
import ru.ifmo.genetics.io.sources.NamedSource;
import ru.ifmo.genetics.utils.iterators.DnaQReaderFromDnaSource;
import ru.ifmo.genetics.io.IOUtils;
import ru.ifmo.genetics.io.readers.FastaReader;

import java.io.File;

public class Fasta2Binq {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Fasta2Binq <phred> <fasta file> <binq file>");
            System.exit(1);
        }

        int phred = Integer.parseInt(args[0]);

        NamedSource<Dna> source = new FastaReader(new File(args[1]));
        NamedSource<DnaQ> dnaqSource = new DnaQReaderFromDnaSource(source, phred);
        IOUtils.dnaQs2BinqFile(dnaqSource, args[2]);
    }
}
