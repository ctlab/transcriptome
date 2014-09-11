package ru.ifmo.genetics.tools.olc.optimizer;


import ru.ifmo.genetics.dna.Dna;
import ru.ifmo.genetics.io.readers.ReadsPlainReader;
import ru.ifmo.genetics.statistics.Timer;
import ru.ifmo.genetics.executors.BlockingThreadPoolExecutor;
import ru.ifmo.genetics.tools.olc.overlaps.OptimizingTask;
import ru.ifmo.genetics.tools.olc.overlaps.Overlaps;
import ru.ifmo.genetics.tools.olc.overlaps.OverlapsList;
import ru.ifmo.genetics.utils.tool.ExecutionFailedException;
import ru.ifmo.genetics.utils.tool.Parameter;
import ru.ifmo.genetics.utils.tool.Tool;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.FileParameterBuilder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import static ru.ifmo.genetics.utils.NumUtils.groupDigits;

public class OverlapsOptimizer extends Tool {
    public static final String NAME = "overlaps-optimizer";
    public static final String DESCRIPTION = "optimizes overlaps by removing transitive overlaps";


    // input params
    public final Parameter<File> readsFile = addParameter(new FileParameterBuilder("reads-file")
            .mandatory()
            .withDescription("file with all reads")
            .create());

    public final Parameter<File> overlapsFile = addParameter(new FileParameterBuilder("overlaps-file")
            .mandatory()
            .withDescription("file with all overlaps")
            .create());

    public final Parameter<File> optimizedOverlapsFile = addParameter(new FileParameterBuilder("optimized-overlaps-file")
            .optional()
            .withDefaultValue(workDir.append("overlaps.optimized"))
            .withDescription("file with optimized overlaps with weight")
            .create());


    // internal variables
    private int readsNumber;
    private ArrayList<Dna> reads;
    private Overlaps overlaps;
    private Overlaps newOverlaps;


    @Override
    protected void runImpl() throws ExecutionFailedException {
        try {
            load();
            sortOverlaps();
            optimizeOverlaps();
            newOverlaps.printToFile(optimizedOverlapsFile.get().toString());
        } catch (IOException e) {
            throw new ExecutionFailedException(e);
        } catch (InterruptedException e) {
            throw new ExecutionFailedException(e);
        }
    }

    private void load() throws IOException, InterruptedException {
        info("Loading reads...");
        reads = ReadsPlainReader.loadReadsAndAddRC(readsFile.get().toString());
        readsNumber = reads.size();

        info("Loading overlaps...");
        overlaps = new Overlaps(reads, new File[]{overlapsFile.get()}, availableProcessors.get());
    }
    
    private void sortOverlaps() throws InterruptedException {
        info("Sorting overlaps...");
        overlaps.sortAll();
    }

    private void optimizeOverlaps() throws InterruptedException {
        info("Optimizing overlaps...");
        Timer timer = new Timer();
        long bs = overlaps.calculateSize();

        newOverlaps = new Overlaps(overlaps, false, true);

        BlockingThreadPoolExecutor executor = new BlockingThreadPoolExecutor(availableProcessors.get());
        int taskSize = readsNumber / availableProcessors.get() + 1;
//        int taskSize = readsNumber / 1 + 1;
        for (int i = 0; i < readsNumber; i += taskSize) {
            executor.blockingExecute(
                    new OptimizingTask(overlaps, newOverlaps, i, Math.min(i + taskSize, readsNumber)));
        }
        executor.shutdownAndAwaitTermination();

        calculateWeight();


        long os = newOverlaps.calculateSize();
        String s = "After optimizing " + groupDigits(os) + " overlaps";
        if (bs != 0) {
            s += String.format(" = %.1f%% of all", 100.0 * os / bs);
        }
        info(s);
        info("Optimizing overlaps took " + timer);
    }


    private void calculateWeight() {
        info("Calculating weight...");
//        QuantitativeStatistics<Integer> weightStat = new QuantitativeStatistics<Integer>();
//        QuantitativeStatistics<Integer> weightGoodStat = new QuantitativeStatistics<Integer>();
//        QuantitativeStatistics<Integer> weightBadStat = new QuantitativeStatistics<Integer>();
        for (int i = 0; i < newOverlaps.readsNumber; i++) {
            if (newOverlaps.getList(i) != null) {
                OverlapsList list = newOverlaps.getList(i);
                for (int j = 0; j < list.size(); j++) {
                    int to = list.getTo(j);
                    int centerShift = list.getCenterShift(j);
                    int overlap = newOverlaps.calculateOverlapLen(i, to, centerShift);

                    list.setWeight(j, overlap);

//                    weightStat.add(weight);
//                    if (checker.checkOverlap(i, to, centerShift)) {
//                        weightGoodStat.add(weight);
//                    } else {
//                        weightBadStat.add(weight);
//                    }
                }
            }
        }
//        weightStat.printToFile("work/weight.stat", null);
//        weightGoodStat.printToFile("work/weight_good.stat", null);
//        weightBadStat.printToFile("work/weight_bad.stat", null);
    }




    @Override
    protected void clean() throws ExecutionFailedException {
        reads = null;
        overlaps = null;
        newOverlaps = null;
    }

    public OverlapsOptimizer() {
        super(NAME, DESCRIPTION);
    }

}
