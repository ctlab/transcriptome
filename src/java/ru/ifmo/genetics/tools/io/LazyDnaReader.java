package ru.ifmo.genetics.tools.io;

import ru.ifmo.genetics.dna.Dna;
import ru.ifmo.genetics.io.formats.Sanger;
import ru.ifmo.genetics.io.readers.BinqReader;
import ru.ifmo.genetics.io.readers.FastaReader;
import ru.ifmo.genetics.io.readers.FastaReaderFromXQSource;
import ru.ifmo.genetics.io.readers.FastqReader;
import ru.ifmo.genetics.io.sources.NamedSource;
import ru.ifmo.genetics.utils.tool.ExecutionFailedException;
import ru.ifmo.genetics.utils.tool.Parameter;
import ru.ifmo.genetics.utils.tool.Tool;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.FileParameterBuilder;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.StringParameterBuilder;
import ru.ifmo.genetics.utils.tool.values.InMemoryValue;
import ru.ifmo.genetics.utils.tool.values.InValue;

import java.io.File;
import java.io.IOException;

public class LazyDnaReader extends Tool {
    public static final String NAME = "dna-reader";
    public static final String DESCRIPTION = "reads dnas from any file";


    // input params
    public final Parameter<File> fileIn = addParameter(new FileParameterBuilder("in-file")
            .mandatory()
            .withShortOpt("i")
            .withDescription("file to read Dnas from")
            .create());

    public Parameter<String> fileFormatIn = addParameter(new StringParameterBuilder("in-format")
            .optional()
            .withDescription("input file format")
            .withDefaultValue(new FileFormatYielder(fileIn))
            .create());


    // internal variables
    private InMemoryValue<NamedSource<Dna>> dnasSource = new InMemoryValue<NamedSource<Dna>>();

    // output params
    public InValue<NamedSource<Dna>> dnasSourceOut = dnasSource.inValue();


    @Override
    protected void runImpl() throws ExecutionFailedException {
        String fileFormat = fileFormatIn.get().toLowerCase();
        try {
            if (fileFormat.equals("fasta")) {
                dnasSource.set(new FastaReader(fileIn.get()));
            } else if (fileFormat.equals("fastq")) {
                dnasSource.set(new FastaReaderFromXQSource(
                        new FastqReader(fileIn.get(), new Sanger())));
            } else if (fileFormat.equals("binq")) {
                dnasSource.set(new FastaReaderFromXQSource(
                        new BinqReader(fileIn.get())));
            } else {
                throw new ExecutionFailedException("Illegal format " + fileFormat);
            }
        } catch (IOException e) {
            throw new ExecutionFailedException("Can't create library from file " + fileIn.get());
        }
    }


    public static NamedSource<Dna> sourceFromFile(File file) throws ExecutionFailedException {
        LazyDnaReader r = new LazyDnaReader();
        r.fileIn.set(file);
        r.simpleRun();
        return r.dnasSourceOut.get();
    }

    public static NamedSource<Dna> sourceFromFile(String fileName) throws ExecutionFailedException {
        return sourceFromFile(new File(fileName));
    }


    @Override
    protected void clean() throws ExecutionFailedException {
    }

    public LazyDnaReader() {
        super(NAME, DESCRIPTION);
    }


}
