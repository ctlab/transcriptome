package ru.ifmo.genetics.tools.rf;

import ru.ifmo.genetics.dna.DnaQ;
import ru.ifmo.genetics.dna.LightDna;
import ru.ifmo.genetics.dna.LightDnaQ;
import ru.ifmo.genetics.io.DedicatedWriter;
import ru.ifmo.genetics.io.PairedLibraryInfo;
import ru.ifmo.genetics.io.writers.ListDedicatedWriter;
import ru.ifmo.genetics.io.sources.*;
import ru.ifmo.genetics.io.sources.NamedSource;
import ru.ifmo.genetics.statistics.Timer;
import ru.ifmo.genetics.statistics.reporter.LocalMonitor;
import ru.ifmo.genetics.statistics.reporter.LocalReporter;
import ru.ifmo.genetics.structures.debriujn.CompactDeBruijnGraph;
import ru.ifmo.genetics.tools.rf.task.FillingReport;
import ru.ifmo.genetics.tools.rf.task.FillingTask;
import ru.ifmo.genetics.tools.rf.task.GlobalContext;
import ru.ifmo.genetics.executors.BlockingThreadPoolExecutor;
import ru.ifmo.genetics.tools.io.DnaWriter;
import ru.ifmo.genetics.tools.io.LazyDnaQReader;
import ru.ifmo.genetics.utils.IteratorUtils;
import ru.ifmo.genetics.utils.TextUtils;
import ru.ifmo.genetics.utils.iterators.ProgressableIterator;
import ru.ifmo.genetics.utils.pairs.UniPair;
import ru.ifmo.genetics.utils.tool.ExecutionFailedException;
import ru.ifmo.genetics.utils.tool.Parameter;
import ru.ifmo.genetics.utils.tool.Tool;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ReadsFiller extends Tool {
    public static final String NAME = "reads-filler";
    public static final String DESCRIPTION = "fills gaps in paired reads";

    private final static int TASK_SIZE = 1 << 12;

    public final Parameter<Integer> kParameter = addParameter(new IntParameterBuilder("k")
            .mandatory()
            .withShortOpt("k")
            .withDescription("k-mer size (vertex, not edge)")
            .create());

    public final Parameter<Integer> minInsertSize = addParameter(new IntParameterBuilder("min-size")
            .optional()
            .withShortOpt("l")
            .withDefaultValue(0)
            .withDescription("minimal insert size of paired-end library to check")
            .create());

    public final Parameter<Integer> maxInsertSize = addParameter(new IntParameterBuilder("max-size")
            .optional()
            .withShortOpt("L")
            .withDefaultValue(1000)
            .withDescription("maximal insert size of paired-end library to check")
            .create());

    /*
    public final Parameter<Boolean> printNs = addParameter(new BoolParameterBuilder("print-ns")
            .optional()
            .withDescription("if set prints 'N' when nucleotide is ambigous")
            .create());
    */


    public final Parameter<File> graphFile = addParameter(new FileParameterBuilder("graph-file")
            .mandatory()
            .withShortOpt("g")
            .withDescription("file with De Bruijn graph")
            .create());

    public final Parameter<File[]> readFiles = addParameter(new FileMVParameterBuilder("read-files")
            .mandatory()
            .withShortOpt("i")
            .withDescription("files with paired reads")
            .create());

    public final Parameter<String[]> sOrientationsToCheck = addParameter(new StringMVParameterBuilder("orientations")
            .optional()
            .withDescription("list of orientations to try to assemble, variants are RF, FR, FF, RR")
            .withDefaultValue(new String[] {"FR"})
            .create());

    public final Parameter<File> outputDir = addParameter(new FileParameterBuilder("output-dir")
            .optional()
            .withShortOpt("o")
            .withDescription("directory to output built quasicontigs")
            .withDefaultValue(workDir.append("quasicontigs"))
            .create());


    private int k;
    private ArrayList<Orientation> orientationsToCheck;

    private CompactDeBruijnGraph graph;


    @Override
    protected void runImpl() throws ExecutionFailedException {
        k = kParameter.get();
        outputDir.get().mkdirs();
        orientationsToCheck = new ArrayList<Orientation>();
        for (String s: sOrientationsToCheck.get()) {
            orientationsToCheck.add(Orientation.fromString(s.toUpperCase()));
        }

        Timer timer = new Timer();
        timer.start();

        info("Loading graph...");
        try {
            FileInputStream fis = new FileInputStream(graphFile.get());
            DataInputStream dis = new DataInputStream(new BufferedInputStream(fis));
            graph = new CompactDeBruijnGraph();
            graph.readFields(dis);
            dis.close();
        } catch (IOException e) {
            throw new ExecutionFailedException(e);
        }

        info("Loading graph done, it took " + timer.finish()/1000. + " seconds");

        try {
            File[] files = readFiles.get();

            ArrayList<PairedLibrary<? extends LightDnaQ>> libraries = splitFilesToLibraries(files);

            fillReadsInLibraries(libraries);
        } catch (InterruptedException e) {
            throw new ExecutionFailedException(e);
        } catch (IOException e) {
            throw new ExecutionFailedException(e);
        }
        info("total time = " + timer.finish());
    }

    private ArrayList<PairedLibrary<? extends LightDnaQ>> splitFilesToLibraries(File[] files) throws ExecutionFailedException {
        assert files.length % 2 == 0;

        info("Splitting files to paired libraries");

        final int MAX_HAMMING_DISTANCE = 1;
        final int SAMPLE_SIZE = 1000;

        ArrayList<PairedLibrary<? extends LightDnaQ>> libraries = new ArrayList<PairedLibrary<? extends LightDnaQ>>();

        LazyDnaQReader dnaQReader = new LazyDnaQReader();

        int n = files.length;

        ArrayList<NamedSource<? extends LightDnaQ>> sources = new ArrayList<NamedSource<? extends LightDnaQ>>(n);
        for (File file : files) {
            dnaQReader.fileIn.set(file);
            dnaQReader.simpleRun();
            NamedSource<? extends LightDnaQ> source = dnaQReader.dnaQsSourceOut.get();
            sources.add(source);
        }

        int[][] d = new int[n][n];
        PairedLibraryInfo[][] stats = new PairedLibraryInfo[n][n];



        for (int i = 0; i < n; ++i) {
            for (int j = i + 1; j < n; ++j) {
                if (TextUtils.hammingDistance(sources.get(i).name(), sources.get(j).name()) > MAX_HAMMING_DISTANCE) {
                    // :ToDo: check all pairs?
                    continue;
                }

                FillingReport report = new FillingReport();
                ArrayList<LightDna> results = new ArrayList<LightDna>();

                DedicatedWriter<LightDna> writer = new ListDedicatedWriter<LightDna>(results);
                Queue<List<UniPair<DnaQ>>> writeFailedQueue = new ConcurrentLinkedQueue<List<UniPair<DnaQ>>>();

                GlobalContext env = new GlobalContext(writer, writeFailedQueue, k,
                        minInsertSize.get(), maxInsertSize.get(), graph,
                        orientationsToCheck,
                        new LocalReporter<FillingReport>(report)
                );

                NamedSource<? extends LightDnaQ> source1 = sources.get(i);
                NamedSource<? extends LightDnaQ> source2 = sources.get(j);

                PairSource<LightDnaQ> pairedSource = PairSource.create(source1, source2);
                List<? extends UniPair<? extends LightDnaQ>> task = IteratorUtils.head(SAMPLE_SIZE, pairedSource.iterator());

                String name = source1.name() + " and " + source2.name();
                debug("Checking as paired " + name);

                FillingTask ft = new FillingTask(env, task);
                ft.runImpl();
                d[j][i] = d[i][j] = (int) ft.getOk();
                debug("Got " + d[i][j] + " ok for library " + name);

                if (d[i][j] > 2) {
                    ArrayList<Integer> lengths = new ArrayList<Integer>(results.size());
                    for (LightDna dna: results) {
                        lengths.add(dna.length());
                    }

                    Collections.sort(lengths);
                    int minSize = lengths.get(0);
                    int maxSize = lengths.get(lengths.size() - 1);
                    int sum = 0;
                    for (Integer l: lengths) {
                        sum += l;
                    }
                    int avgSize = sum / lengths.size();

                    long sumSq = 0;
                    for (Integer l: lengths) {
                        long x = (long) l - avgSize;
                        sumSq += x * x;
                    }
                    int stdDev = (int)Math.sqrt(sumSq / (lengths.size() - 1));
                    minSize = Math.max(0, minSize - stdDev);
                    maxSize += stdDev;
                    stats[j][i] = stats[i][j] = new PairedLibraryInfo(minSize, maxSize, avgSize, stdDev);
                }
            }
        }

        int[] best = new int[n];

        for (int i = 0; i < n; ++i) {
            int max = -1;
            for (int j = 0; j < n; ++j) {
                if (d[i][j] > max) {
                    max = d[i][j];
                    best[i] = j;
                }
            }
        }

        for (int i = 0; i < n; ++i) {
            int j = best[i];
            if (best[j] != i) {
                warn("Source " + sources.get(i).name() + " seems not to be paired, skipping");
                continue;
            }
            if (i > j) {
                continue;
            }

            ZippingPairedLibrary<LightDnaQ> library = ZippingPairedLibrary.create(sources.get(i), sources.get(j), stats[i][j]);
            info("Found paired-end library: " + library.name() + " (" + stats[i][j] + ")");
            libraries.add(library);
        }
        return libraries;
    }

    private void processPairedLibrary(PairedLibrary<? extends LightDnaQ> library, BlockingThreadPoolExecutor pool, GlobalContext env) throws InterruptedException {
        int pairCounter = 0;
        List<UniPair<? extends LightDnaQ>> task = new ArrayList<UniPair<? extends LightDnaQ>>(TASK_SIZE);

        int progress;
        int lastProgress = -1;
        final int PROGRESS_MAX = 1 << 13;
        createProgressBar(PROGRESS_MAX);

        for (ProgressableIterator<? extends UniPair<? extends LightDnaQ>> it = library.iterator(); it.hasNext();) {
            UniPair<? extends LightDnaQ> pair = it.next();
            pairCounter++;


            task.add(pair);
            if (task.size() == TASK_SIZE) {
                pool.blockingExecute(new FillingTask(env, task));
                progress = (int)(it.progress() * PROGRESS_MAX);
                updateProgressBar(progress);
                /*
                if (progress != lastProgress) {
                    updateProgressBar(progress);
                    lastProgress = progress;
                }
                */

                task = new ArrayList<UniPair<? extends LightDnaQ>>(TASK_SIZE);
            }
        }

        destroyProgressBar();

        if (task.size() != 0) {
            pool.blockingExecute(new FillingTask(env, task));
        }

    }

    private void fillReadsInLibraries(ArrayList<PairedLibrary<? extends LightDnaQ>> libraries) throws InterruptedException, IOException, ExecutionFailedException {
        Timer timer = new Timer();
        timer.start();

        Timer oneDatasetTimer = new Timer();

        int fileId = 0;

        Queue<List<UniPair<DnaQ>>> writeFailedQueue = new ConcurrentLinkedQueue<List<UniPair<DnaQ>>>();

        FillingReport report = new FillingReport();
        LocalMonitor monitor = new LocalMonitor(report);
        monitor.start();

        int n = libraries.size();

        DnaWriter dnaWriter = new DnaWriter();

        for (PairedLibrary<? extends LightDnaQ> library: libraries) {
            info("Processing library " + library.name());

            // ExecutorService pool = Executors.newFixedThreadPool(availableProcessors);
            BlockingThreadPoolExecutor pool = new BlockingThreadPoolExecutor(availableProcessors.get());

//            debug(outputDir.get().toString());
//            debug(printNs.get());
            dnaWriter.fileBaseNameIn.set(new File(outputDir.get(), library.name()));
            dnaWriter.simpleRun();
            DedicatedWriter<LightDna> writer = dnaWriter.dnaWriterOut.get();


            GlobalContext env = new GlobalContext(writer, writeFailedQueue, k,
                    library.info().minSize, library.info().maxSize, graph,
                    orientationsToCheck,
                    new LocalReporter<FillingReport>(report)
            );

            Thread writingFailedThread =
                    new Thread(new PairWriter(writeFailedQueue, outputDir.get().toString() + File.separator + library.name() + "_failed"));
            writer.start();
            writingFailedThread.start();
            oneDatasetTimer.start();

            processPairedLibrary(library, pool, env);

            debug("Dataset read, waiting for termination");

            pool.shutdownAndAwaitTermination();


            List<UniPair<DnaQ>> endFailedList = new ArrayList<UniPair<DnaQ>>(1);
            endFailedList.add(null);

            writeFailedQueue.add(endFailedList);

            writer.stopAndWaitForFinish();
            writingFailedThread.join();

            ++fileId;
            info("name = " + library.name() + ", fileId/library.size = " + fileId + "/" + n);
            info("time = " + oneDatasetTimer);

            double done = ((double) fileId) / n;
            long total = (long) (timer.finish() / done);
            long remained = (long) (total * (1 - done));
            long elapsed = (long) (total * done);

//            info(100 * done + "% done");
            info("estimated  total time: " + Timer.timeToStringWithoutMs(total) + ", " +
                    "remaining: " + Timer.timeToStringWithoutMs(remained) + ", " +
                    "elapsed: " + Timer.timeToStringWithoutMs(elapsed));

            info("Statistics: " + report.toString());
            report.reset();
        }

        monitor.stop();

    }

    @Override
    protected void clean() throws ExecutionFailedException {
        orientationsToCheck = null;
        graph = null;
    }

    public static void main(String args[]) {
        new ReadsFiller().mainImpl(args);
    }

    public ReadsFiller() {
        super(NAME, DESCRIPTION);
    }
}
