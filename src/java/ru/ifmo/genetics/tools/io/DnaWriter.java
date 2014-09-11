package ru.ifmo.genetics.tools.io;

import org.apache.commons.net.ftp.FTPListParseEngine;
import ru.ifmo.genetics.dna.LightDna;
import ru.ifmo.genetics.io.DedicatedWriter;
import ru.ifmo.genetics.io.writers.FastaDedicatedWriter;
import ru.ifmo.genetics.utils.tool.ExecutionFailedException;
import ru.ifmo.genetics.utils.tool.Parameter;
import ru.ifmo.genetics.utils.tool.Tool;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.FileParameterBuilder;
import ru.ifmo.genetics.utils.tool.values.InMemoryValue;
import ru.ifmo.genetics.utils.tool.values.InValue;
import ru.ifmo.genetics.utils.tool.values.Yielder;

import java.io.File;

public class DnaWriter extends Tool {
    public static final String NAME = "dna-writer";
    public static final String DESCRIPTION = "writes dnas to file";


    // input params
    public final Parameter<File> fileBaseNameIn = addParameter(new FileParameterBuilder("out-file-basename")
            .optional()
            .withDescription("base name of a file to write sequences to")
            .create());

    // input params
    public final Parameter<File> fileIn = addParameter(new FileParameterBuilder("out-file")
            .optional()
            .withShortOpt("o")
            .withDescription("a file to write sequences to")
            .withDefaultValue(
                    new Yielder<File>() {
                        @Override
                        public File yield() {
                            if (fileBaseNameIn.get() == null) {
                                return null;
                            }
                            return new File(fileBaseNameIn.get().toString() + ".fasta");
                        }

                        @Override
                        public String description() {
                            return "appends .fasta to base name";
                        }
                    })
            .create());

    // internal variables
    private InMemoryValue<DedicatedWriter<LightDna>> dnaWriter = new InMemoryValue<DedicatedWriter<LightDna>>();

    // output params
    public InValue<DedicatedWriter<LightDna>> dnaWriterOut = dnaWriter.inValue();




    @Override
    protected void runImpl() throws ExecutionFailedException {
        dnaWriter.set(new FastaDedicatedWriter(fileIn.get()));
    }


    @Override
    protected void clean() throws ExecutionFailedException {
    }

    public DnaWriter() {
        super(NAME, DESCRIPTION);
    }

    public static DedicatedWriter<LightDna> getWriterForBaseName(File baseName) throws ExecutionFailedException {
        DnaWriter dnaWriter = new DnaWriter();
        dnaWriter.fileBaseNameIn.set(baseName);
        dnaWriter.simpleRun();
        return dnaWriter.dnaWriterOut.get();
    }
}
