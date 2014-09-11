package ru.ifmo.genetics.tools.io;

import ru.ifmo.genetics.dna.DnaQ;
import ru.ifmo.genetics.io.sources.NamedSource;
import ru.ifmo.genetics.utils.iterators.DnaQReaderFromDnaSource;
import ru.ifmo.genetics.io.formats.QualityFormatFactory;
import ru.ifmo.genetics.io.readers.BinqReader;
import ru.ifmo.genetics.io.readers.FastaReader;
import ru.ifmo.genetics.io.readers.FastqReader;
import ru.ifmo.genetics.utils.tool.*;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.FileParameterBuilder;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.IntParameterBuilder;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.StringParameterBuilder;
import ru.ifmo.genetics.utils.tool.Parameter;
import ru.ifmo.genetics.utils.tool.values.InMemoryValue;
import ru.ifmo.genetics.utils.tool.values.InValue;
import ru.ifmo.genetics.utils.tool.values.ToStringYielder;
import ru.ifmo.genetics.utils.tool.values.Yielder;

import java.io.File;
import java.io.IOException;

public class LazyDnaQReader extends Tool {
    public static final String NAME = "dnaq-reader";
    public static final String DESCRIPTION = "reads dnaqs from any file";


    // input params
    public final Parameter<File> fileIn = addParameter(new FileParameterBuilder("in-file")
            .mandatory()
            .withShortOpt("i")
            .withDescription("file to read DnaQs from")
            .create());

    public Parameter<String> fileFormatIn = addParameter(new StringParameterBuilder("in-format")
            .optional()
            .withDescription("input file format")
            .withDefaultValue(new FileFormatYielder(fileIn))
            .create());

    public QualityFormatDeterminer qualityFormatDeterminer = new QualityFormatDeterminer();
    {
        setFix(qualityFormatDeterminer.inFile, fileIn);
        setFixDefault(qualityFormatDeterminer.head);
        addSubTool(qualityFormatDeterminer);
    }

    public Parameter<String> qualityFormatIn = addParameter(new StringParameterBuilder("in-qformat")
            .optional()
            .withDescription("input file quality format (for fastq)")
            .withDefaultValue(ToStringYielder.create(qualityFormatDeterminer.qualityFormatOut))
            .create());


    public Parameter<Integer> setPhred = addParameter(new IntParameterBuilder("set-phred")
            .optional()
            .withDescription("sets phred quality for fasta files")
            .withDefaultValue(20)
            .create());


    // internal variables
    private InMemoryValue<NamedSource<DnaQ>> dnaQsSource = new InMemoryValue<NamedSource<DnaQ>>();

    // output params
    public InValue<NamedSource<DnaQ>> dnaQsSourceOut = dnaQsSource.inValue();


    @Override
    protected void runImpl() throws ExecutionFailedException {
        String fileFormat = fileFormatIn.get().toLowerCase();
        try {
            if (fileFormat.equals("fastq")) {
                qualityFormatDeterminer.simpleRun();
                dnaQsSource.set(new FastqReader(fileIn.get(), QualityFormatFactory.instance.get(qualityFormatIn.get())));
            } else if (fileFormat.equals("fasta")) {
                dnaQsSource.set(new DnaQReaderFromDnaSource(new FastaReader(fileIn.get()), setPhred.get()));
            } else if (fileFormat.equals("binq")) {
                dnaQsSource.set(new BinqReader(fileIn.get()));
            } else {
                throw new ExecutionFailedException("Illegal format " + fileFormat);
            }
        } catch (IOException e) {
            throw new ExecutionFailedException("Can't create library from file " + fileIn.get());
        }
    }


    public static NamedSource<DnaQ> sourceFromFile(File file) throws ExecutionFailedException {
        LazyDnaQReader r = new LazyDnaQReader();
        r.fileIn.set(file);
        r.simpleRun();
        return r.dnaQsSourceOut.get();
    }

    public static NamedSource<DnaQ> sourceFromFile(String fileName) throws ExecutionFailedException {
        return sourceFromFile(new File(fileName));
    }


    @Override
    protected void clean() throws ExecutionFailedException {
    }

    public LazyDnaQReader() {
        super(NAME, DESCRIPTION);
    }


}
