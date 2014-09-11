package ru.ifmo.genetics.tools.converters;

import ru.ifmo.genetics.io.IOUtils;
import ru.ifmo.genetics.io.readers.BinqReader;

public class Binq2Fastq {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Usage: Binq2Fastq <binq file> [<output file name>]");
            System.exit(1);
        }
        String inFile = args[0];
        String outFile;
        final String BINQ_SUFFIX = ".binq";
        if (args.length > 1) {
            outFile = args[1];
        } else if (inFile.endsWith(BINQ_SUFFIX)) {
            outFile = inFile.substring(0, inFile.length() - BINQ_SUFFIX.length()) + ".fastq";
        } else {
            outFile = inFile + ".fastq";
        }

        BinqReader binqReader = new BinqReader(args[0]);
        IOUtils.dnaQs2FastqFile(binqReader, args[0], outFile);
    }
}
