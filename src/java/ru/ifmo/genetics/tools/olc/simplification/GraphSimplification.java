package ru.ifmo.genetics.tools.olc.simplification;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import ru.ifmo.genetics.dna.Dna;
import ru.ifmo.genetics.dna.kmers.Kmer;
import ru.ifmo.genetics.dna.kmers.KmerIteratorFactory;
import ru.ifmo.genetics.dna.kmers.ShortKmerIteratorFactory;
import ru.ifmo.genetics.io.readers.ReadsPlainReader;
import ru.ifmo.genetics.statistics.QuantitativeStatistics;
import ru.ifmo.genetics.structures.ArrayLong2IntHashMap;
import ru.ifmo.genetics.tools.olc.ReadsGenerator;
import ru.ifmo.genetics.tools.olc.CheckerFromRef;
import ru.ifmo.genetics.tools.olc.overlaps.Overlaps;
import ru.ifmo.genetics.utils.tool.ExecutionFailedException;
import ru.ifmo.genetics.utils.tool.Parameter;
import ru.ifmo.genetics.utils.tool.Tool;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.FileMVParameterBuilder;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.FileParameterBuilder;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.IntParameterBuilder;
import ru.ifmo.genetics.utils.tool.values.InMemoryValue;
import ru.ifmo.genetics.utils.tool.values.InValue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class GraphSimplification extends Tool {
    public static final String NAME = "overlap-graph-simplification";
    public static final String DESCRIPTION = "simplifies overlap graph using coverage and repeat model";


    // input parameters
    public final Parameter<File[]> initialReads = addParameter(new FileMVParameterBuilder("initial-reads")
            .mandatory()
            .withShortOpt("i")
            .withDescription("initial paired-end binq files")
            .create());

    public final Parameter<Integer> k = addParameter(new IntParameterBuilder("k")
            .mandatory()
            .withShortOpt("k")
            .withDescription("k-mer size")
            .create());

    public final Parameter<File> readsFile = addParameter(new FileParameterBuilder("reads-file")
            .mandatory()
            .withShortOpt("r")
            .withDescription("file with quasi-contigs")
            .create());

    public final Parameter<File> overlapsFile = addParameter(new FileParameterBuilder("overlaps-file")
            .mandatory()
            .withShortOpt("o")
            .withDescription("file with overlaps")
            .create());


    // internal variables
    public KmerStatisticsGatherer gatherer = new KmerStatisticsGatherer();
    {
        setFix(gatherer.k, k);
        setFix(gatherer.inputFiles, initialReads);
        addSubTool(gatherer);
    }
    private ArrayLong2IntHashMap hm;

    private QuantitativeStatistics<Integer> hmDistr;
    private QuantitativeStatistics<Integer> coverageDistr;

    private int readsNumber;
    private ArrayList<Dna> reads;
    private Overlaps overlaps;
    private CheckerFromRef checker;


    // output parameters
    private final InMemoryValue<Integer> thresholdOutValue = new InMemoryValue<Integer>();
    public final InValue<Integer> thresholdOut = addOutput("threshold", thresholdOutValue, Integer.class);



    @Override
    protected void runImpl() throws ExecutionFailedException {
        try {
            gatherer.simpleRun();

            hm = gatherer.hmOut.get();
            System.out.println("hm.size = " + hm.size());

            hmDistr = getQSFromHM(hm);
            hmDistr.printToFile(workDir.append("hm-distribution").toString());


            load();

            calculateCoverage();

            test();

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

        info("Loading reads' info...");
        checker = new CheckerFromRef();

        info("Loading overlaps...");
        overlaps = new Overlaps(reads, new File[]{overlapsFile.get()}, availableProcessors.get());
    }

    QuantitativeStatistics<Integer> getQSFromHM(ArrayLong2IntHashMap hm) {
        QuantitativeStatistics<Integer> stat = new QuantitativeStatistics<Integer>();
        for (Long2IntOpenHashMap lm : hm.hm) {
            for (int v : lm.values()) {
                stat.add(v);
            }
        }
        return stat;
    }

    private void calculateCoverage() {
        info("Calculating coverage...");

        KmerIteratorFactory<Kmer> factory = new ShortKmerIteratorFactory();

        QuantitativeStatistics<Integer> meanDistr = new QuantitativeStatistics<Integer>();
        QuantitativeStatistics<Integer> varDistr = new QuantitativeStatistics<Integer>();

        createProgressBar(reads.size());
        int genomePos = 1430580;
        int badReadsCount = 0;
        for (int i = 0; i < reads.size(); i++) {
            Dna d = reads.get(i);
            ReadsGenerator.ReadInfo info = checker.getReadInfo(i);
            QuantitativeStatistics<Integer> coverage = new QuantitativeStatistics<Integer>();

            if (info.beginPos <= genomePos && info.beginPos + info.len > genomePos) {
                System.out.println();
                System.out.println("Stat for read # " + i);
                System.out.println("dna = " + d);
//                System.out.println("RC_dna = " + d.reverseComplement());
                System.out.println("dna.len = " + d.length());
                System.out.println("begin = " + info.beginPos + ", end = " + (info.beginPos + info.len) + ", rc = " + info.rc);
                System.out.println("Kmers coverage:");
            }

            int c = 0;
            boolean bad = false;
            for (Kmer kmer : factory.kmersOf(d, k.get())) {

                int val = hm.get(kmer.toLong());


                if (info.beginPos <= genomePos && info.beginPos + info.len > genomePos) {
//                    out.print((c / 100 % 10) + "" + (c / 10 % 10) + "" + c%10 + " = ");
//                    for (int cc = 0; cc < c; cc++) {
//                        out.print(' ');
//                    }
//                    out.println(DnaTools.toString(kmer));
                    c++;
                    System.out.print(" " + val);

//                    if (Math.random() < 0.1) {
//                        System.err.print(c + ":\t val = " + val + ", \tkmer = " + DnaTools.toString(kmer) + ", ");
//                        long rcKmerL = ((ShortKmer) kmer).rcKmer();
//                        ShortKmer rcKmer = new ShortKmer(rcKmerL, kmer.length());
//                        int rcVal = hm.get(rcKmerL);
//                        System.err.println("RC: val = " + rcVal + ", \tkmer = " + DnaTools.toString(rcKmer));
//                    }
                }

                if (val == 0) {
                    bad = true;
                }
                
                coverage.add(val);
            }

            if (bad) {
                badReadsCount++;
            }

            double cov = coverage.calculateMean();
            meanDistr.add((int) cov);
            double var = coverage.calculateVariance();
            varDistr.add((int) var);

            if (info.beginPos <= genomePos && info.beginPos + info.len > genomePos) {
                System.out.println();
                System.out.println("cov = " + cov);
                System.out.println("var = " + var);
                System.out.println();
            }

            updateProgressBar(i + 1);
        }
        destroyProgressBar();

        System.err.println("Bad reads count = " + badReadsCount + " = " + String.format("%.1f", 100.0 * badReadsCount / reads.size()) + "% of all");

        System.err.println("Done, dumping...");
        meanDistr.printToFile(workDir.append("coverage-distribution").toString());
        varDistr.printToFile(workDir.append("coverage-var-distribution").toString());

        coverageDistr = meanDistr;
    }


    private void test() {

    }


    @Override
    protected void clean() throws ExecutionFailedException {
        gatherer = null;
        hm = null;
        reads = null;
    }

    public GraphSimplification() {
        super(NAME, DESCRIPTION);
    }

    public static void main(String[] args) {
        new GraphSimplification().mainImpl(args);
    }
}
