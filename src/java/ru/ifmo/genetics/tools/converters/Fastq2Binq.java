package ru.ifmo.genetics.tools.converters;

import ru.ifmo.genetics.dna.DnaQ;
import ru.ifmo.genetics.io.formats.QualityFormat;
import ru.ifmo.genetics.io.formats.QualityFormatFactory;
import ru.ifmo.genetics.io.readers.FastqReader;
import ru.ifmo.genetics.io.sources.Source;
import ru.ifmo.genetics.io.IOUtils;

import java.io.File;

public class Fastq2Binq {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Fastq2Binq <quality format> <fastq file> <binq file>");
            System.exit(1);
        }

        QualityFormat qf = QualityFormatFactory.instance.get(args[0]);

        Source<DnaQ> source = new FastqReader(new File(args[1]), qf);
        IOUtils.dnaQs2BinqFile(source, args[2]);
    }
}
