package ru.ifmo.genetics.tools.io;

import ru.ifmo.genetics.dna.DnaQ;
import ru.ifmo.genetics.io.formats.IllegalQualityValueException;
import ru.ifmo.genetics.io.formats.Illumina;
import ru.ifmo.genetics.io.formats.QualityFormat;
import ru.ifmo.genetics.io.formats.Sanger;
import ru.ifmo.genetics.io.readers.FastqReader;
import ru.ifmo.genetics.utils.tool.ExecutionFailedException;
import ru.ifmo.genetics.utils.tool.Parameter;
import ru.ifmo.genetics.utils.tool.Tool;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.FileParameterBuilder;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.IntParameterBuilder;
import ru.ifmo.genetics.utils.tool.values.InMemoryValue;
import ru.ifmo.genetics.utils.tool.values.InValue;

import java.io.File;
import java.io.IOException;

public class QualityFormatDeterminer extends Tool {
    public static final String NAME = "qf-determiner";
    public static final String DESCRIPTION = "determines quality format in FASTQ file";


    // in params
    public Parameter<File> inFile = addParameter(new FileParameterBuilder("in-file")
            .mandatory()
            .withShortOpt("i")
            .withDescription("file to read DnaQs from")
            .create());

    public Parameter<Integer> head = addParameter(new IntParameterBuilder("head")
            .optional()
            .withShortOpt("H")
            .withDescription("number of reads to use")
            .withDefaultValue(1000)
            .create());

    // internal variables
    private InMemoryValue<QualityFormat> qualityFormat = new InMemoryValue<QualityFormat>();

    // out params
    public InValue<QualityFormat> qualityFormatOut = qualityFormat.inValue();


    @Override
    protected void clean() throws ExecutionFailedException {
        qualityFormat = null;
    }

    @Override
    protected void runImpl() throws ExecutionFailedException {
        int i = 0;
        FastqReader reader = null;
        try {
            reader = new FastqReader(inFile.get(), Illumina.instance);
        } catch (IOException e) {
            throw new ExecutionFailedException("Can't create library from from file " + inFile.get(), e);
        }

        try {
            for (DnaQ dnaq: reader) {
                i++;
                if (i >= head.get()) {
                    break;
                }
            }
            qualityFormat.set(Illumina.instance);
        } catch (IllegalQualityValueException e) {
            qualityFormat.set(Sanger.instance);
        }
        logger.debug("Determined quality format of file " + inFile.get() + " as " + qualityFormat.get());
    }

    public QualityFormatDeterminer() {
        super(NAME, DESCRIPTION);
    }
}
