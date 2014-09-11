package ru.ifmo.genetics.tools.io;

import ru.ifmo.genetics.io.IOUtils;
import ru.ifmo.genetics.utils.FileUtils;
import ru.ifmo.genetics.utils.tool.ExecutionFailedException;
import ru.ifmo.genetics.utils.tool.Parameter;
import ru.ifmo.genetics.utils.tool.Tool;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.FileMVParameterBuilder;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.FileParameterBuilder;
import ru.ifmo.genetics.utils.tool.values.InMemoryValue;
import ru.ifmo.genetics.utils.tool.values.InValue;

import java.io.File;
import java.io.IOException;

public class ToBinqConverter extends Tool {
    public static final String NAME = "to-binq-converter";
    public static final String DESCRIPTION = "converts input files to binq format";

    public final Parameter<File[]> inputFiles = addParameter(new FileMVParameterBuilder("input-files")
            .mandatory()
            .withDescription("input files")
            .create());

    public final Parameter<File> outputDir = addParameter(new FileParameterBuilder("output-dir")
            .optional()
            .withDefaultValue(workDir.append("converted"))
            .withDescription("directory for output files")
            .create());

    private final InMemoryValue<File[]> convertedReadsOutValue = new InMemoryValue<File[]>();
    public final InValue<File[]> convertedReadsOut = addOutput("converted-reads", convertedReadsOutValue, File[].class);

    @Override
    protected void runImpl() throws ExecutionFailedException {
        outputDir.get().mkdir();

        File[] convertedReads = new File[inputFiles.get().length];
        int i = 0;
        for (File f : inputFiles.get()) {
            info("Converting " + f.getName() + " to binary format...");
            if (f.getName().toLowerCase().endsWith(".binq")) {
                convertedReads[i++] = f;
                continue;
            }
            File newFile = new File(outputDir.get(), FileUtils.baseName(f) + ".binq");
            convertedReads[i++] = newFile;
            LazyDnaQReader r = new LazyDnaQReader();
            r.fileIn.set(f);
            r.simpleRun();
            try {
                IOUtils.dnaQs2BinqFile(r.dnaQsSourceOut.get(), newFile);
            } catch (IOException e) {
                throw new ExecutionFailedException(e);
            }
        }
        convertedReadsOutValue.set(convertedReads);
    }

    @Override
    protected void clean() {

    }

    public ToBinqConverter() {
        super(NAME, DESCRIPTION);
    }

}
