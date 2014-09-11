package ru.ifmo.genetics.tools.olc.gluedDnasString;

import ru.ifmo.genetics.dna.Dna;
import ru.ifmo.genetics.io.readers.ReadsPlainReader;
import ru.ifmo.genetics.utils.tool.ExecutionFailedException;
import ru.ifmo.genetics.utils.tool.Parameter;
import ru.ifmo.genetics.utils.tool.Tool;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.FileParameterBuilder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import static ru.ifmo.genetics.utils.NumUtils.groupDigits;

public class DnaStringGluer extends Tool {
    public static final String NAME = "dnas-string-gluer";
    public static final String DESCRIPTION = "glues DNAs to one string";

    // input params
    public final Parameter<File> readsFile = addParameter(new FileParameterBuilder("reads-file")
            .mandatory()
            .withDescription("reads file")
            .create());

    public final Parameter<File> fullStringFile = new Parameter<File>(new FileParameterBuilder("full-string-file")
            .optional()
            .withDefaultValue(workDir.append("full-string.txt"))
            .withDescription("file to store glued dnas string")
            .create());


    @Override
    protected void runImpl() throws ExecutionFailedException {
        info("Loading...");
        ArrayList<Dna> reads;
        // ToDo: rewrite it to use normal reader
        try {
            reads = ReadsPlainReader.loadReads(readsFile.get().getPath());
        } catch (IOException e) {
            throw new ExecutionFailedException(e);
        }

        info("Creating string...");
        GluedDnasString string = GluedDnasString.createGluedDnasString(reads);
        info("Full string length = " + groupDigits(string.length));


        info("Dumping...");
        string.dump(fullStringFile.get());
    }


    @Override
    protected void clean() throws ExecutionFailedException {
    }

    public DnaStringGluer() {
        super(NAME, DESCRIPTION);
    }
}
