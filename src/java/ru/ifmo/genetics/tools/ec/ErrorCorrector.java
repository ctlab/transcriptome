package ru.ifmo.genetics.tools.ec;

import ru.ifmo.genetics.tools.io.ToBinqConverter;
import ru.ifmo.genetics.utils.Misc;
import ru.ifmo.genetics.utils.tool.Parameter;
import ru.ifmo.genetics.utils.tool.Tool;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.BoolParameterBuilder;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.FileMVParameterBuilder;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.FileParameterBuilder;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.IntParameterBuilder;
import ru.ifmo.genetics.utils.tool.values.IfYielder;

import java.io.File;

public class ErrorCorrector extends Tool {
    public static final String NAME = "error-corrector";
    public static final String DESCRIPTION = "corrects errors";

    public final Parameter<File[]> inputFiles = addParameter(new FileMVParameterBuilder("input-files")
            .mandatory()
            .withShortOpt("i")
            .withDescription("reads to process")
            .create());

    public final Parameter<Integer> k = addParameter(new IntParameterBuilder("k")
            .mandatory()
            .withShortOpt("k")
            .withDescription("k-mer size")
            .create());

    public final Parameter<Integer> maximalSubsNumber = addParameter(new IntParameterBuilder("maximal-subs-number")
            .withDefaultValue(1)
            .withDescription("maximal substitutions number per k-mer")
            .create());

    public final Parameter<Integer> maximalIndelsNumber = addParameter(new IntParameterBuilder("maximal-indels-number")
            .withDefaultValue(0)
            .withDescription("maximal indels number per k-mer")
            .create());

    public final Parameter<File> outputDir = addParameter(new FileParameterBuilder("output-dir")
            .withDefaultValue(workDir.append("corrected"))
            .withShortOpt("o")
            .withDescription("directory for output files")
            .create());

    public final Parameter<Boolean> applyToOriginalReads = addParameter(new BoolParameterBuilder("apply-to-original")
            .optional()
            .withDescription("if set applies fixes to original not truncated reads")
            .create());

    public final ToBinqConverter converter = new ToBinqConverter();
    {
        setFix(converter.inputFiles, inputFiles);
        setFixDefault(converter.outputDir);
        addSubTool(converter);
    }

    public final BinqTruncater truncater = new BinqTruncater();
    {
        setFix(truncater.inputFiles, converter.convertedReadsOut);
        setFixDefault(truncater.outputDir);
        addSubTool(truncater);
    }

    public final KmerStatisticsGatherer gatherer = new KmerStatisticsGatherer();
    {
        setFix(gatherer.inputFiles, truncater.truncatedReadsOut);
        setFixDefault(gatherer.maxSize);
        setFix(gatherer.k, k);
        setFixDefault(gatherer.prefixesFile);
        setFixDefault(gatherer.outputDir);
        addSubTool(gatherer);
    }

    public final CleanAll cleanAll = new CleanAll();
    {
        setFix(cleanAll.prefixesFile, gatherer.prefixesFile);
        setFix(cleanAll.k, k);
        setFix(cleanAll.kmersDir, gatherer.outputDir);
        setFix(cleanAll.maximalIndelsNumber, maximalIndelsNumber);
        setFix(cleanAll.maximalSubsNumber, maximalSubsNumber);
        setFix(cleanAll.badKmersNumber, gatherer.badKmersNumberOut);
        setFixDefault(cleanAll.outputDir);
        addSubTool(cleanAll);
    }

    public FixesApplier fixesApplier = new FixesApplier();
    {
        setFix(fixesApplier.fixes, cleanAll.fixesOut);
        setFix(fixesApplier.k, k);
        setFix(fixesApplier.reads,
                new IfYielder<File[]>(
                        applyToOriginalReads,
                        converter.convertedReadsOut,
                        truncater.truncatedReadsOut
                ));
        setFix(fixesApplier.outputDir, outputDir);
        setFix(fixesApplier.readsNumber, gatherer.readsNumberOut);
        addSubTool(fixesApplier);
    }

    @Override
    protected void runImpl() {
        addStep(converter);
        addStep(truncater);
        addStep(gatherer);
        addStep(cleanAll);
        addStep(fixesApplier);
    }

    @Override
    protected void clean() {
    }

    public ErrorCorrector() {
        super(NAME, DESCRIPTION);
    }

    public static void main(String[] args) {
        new ErrorCorrector().mainImpl(args);
    }

}
