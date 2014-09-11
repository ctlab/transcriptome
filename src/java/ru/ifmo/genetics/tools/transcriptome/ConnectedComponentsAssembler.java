package ru.ifmo.genetics.tools.transcriptome;

import org.apache.commons.lang.mutable.MutableLong;
import ru.ifmo.genetics.statistics.Timer;
import ru.ifmo.genetics.structures.set.BigLongsHashSet;
import ru.ifmo.genetics.structures.set.LongsHashSet;
import ru.ifmo.genetics.transcriptome.CompactDeBruijnGraphWF;
import ru.ifmo.genetics.utils.KmerUtils;
import ru.ifmo.genetics.utils.Misc;
import ru.ifmo.genetics.utils.iterators.IterableIterator;
import ru.ifmo.genetics.utils.tool.ExecutionFailedException;
import ru.ifmo.genetics.utils.tool.Parameter;
import ru.ifmo.genetics.utils.tool.Tool;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.FileParameterBuilder;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.IntParameterBuilder;

import java.io.*;
import java.util.*;

public class ConnectedComponentsAssembler extends Tool {
    public static final String NAME = "connected-components";
    public static final String DESCRIPTION = "assemble connected components in De Bruijn graph";

    // input params
    public final Parameter<File> kmersFileFolder = addParameter(new FileParameterBuilder("kmers-file-folder")
            .mandatory()
            .withDescription("files with (k+1)-mers to add")
            .create());

    public final Parameter<Integer> kParameter = addParameter(new IntParameterBuilder("k")
            .mandatory()
            .withShortOpt("k")
            .withDescription("k-mer size (vertex, not edge)")
            .create());

    public final Parameter<File> outFilePrefix = addParameter(new FileParameterBuilder("file-prefix")
            .withShortOpt("po")
            .withDescription("output prefix")
            .withDefaultValue(workDir.append("/components"))
            .create());

    //constants

    private final int COMPONENTS_MIN_SIZE = 150;

    // internal vars
    private int k;
    private int maxFreq;
    private CompactDeBruijnGraphWF graph;
    private long graphSizeBytes;
    private LongsHashSet wasEdges;

    @Override
    protected void runImpl() throws ExecutionFailedException {

        k = kParameter.get();
        maxFreq = k/2 + k/4;
        Timer timer = new Timer();
        info("Building graph...");
        try {
            buildGraph();
        } catch (IOException e) {
            throw new ExecutionFailedException(e);
        }
        info("Building graph done, it took " + timer);
        timer.start();
        wasEdges = new BigLongsHashSet(graphSizeBytes);
        wasEdges.put(0L);

        int numOfComponents = 0;

        outFilePrefix.get().mkdir();

        Iterator<MutableLong> iter = graph.getIterator();
        for (MutableLong value: new IterableIterator<MutableLong>(iter)) {
            long curE = value.longValue();
            if (!wasEdges.contains(Math.min(curE,graph.reverseComplementEdge(curE)))){
                numOfComponents++;
                try {
                    bfs(curE&graph.vertexMask,outFilePrefix.get().getAbsolutePath()+"/"+numOfComponents);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if (numOfComponents%10000 == 0){
                    info("I found " + numOfComponents + " components");
                }
            }
        }
        info("Done, it took " + timer);

    }

    private boolean checkKmerForSame(long kmer){
        int []fr = new int[4];
        String dna = KmerUtils.kmer2String(kmer,k);
        for (int i = 0; i < k; i++){
            switch(dna.charAt(i)){
                case 'A':
                    fr[0]++;
                    break;
                case 'C':
                    fr[1]++;
                    break;
                case 'G':
                    fr[2]++;
                    break;
                case 'T':
                    fr[3]++;
                    break;
            }
            for (int j = 0; j < fr.length; j++){
                if (fr[j] > maxFreq){
                    return true;
                }
            }
        }
        return false;
    }

    private boolean bfs(long start, String fnout) throws IOException {
        DataOutputStream outEdges = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fnout)));
        Queue<Long> queue = new LinkedList<Long>();
        queue.add(start);
        int size = 0;
        while(!queue.isEmpty()){
            long cur = queue.poll();
            if ((graph.outcomeEdges(cur).length == 4) || (graph.incomeEdges(cur).length == 4) || (graph.outcomeEdges(cur).length + graph.incomeEdges(cur).length > 5) ||checkKmerForSame(cur)){
                for (long outcome : graph.outcomeEdges(cur)){
                    wasEdges.put(Math.min(outcome,graph.reverseComplementEdge(outcome)));
                }
                for (long income : graph.incomeEdges(cur)){
                    wasEdges.put(Math.min(income,graph.reverseComplementEdge(income)));
                }
                continue;
            }

            for (long outcome : graph.outcomeEdges(cur)){
                if (!wasEdges.contains(Math.min(outcome,graph.reverseComplementEdge(outcome)))){
                    long outV = outcome&graph.vertexMask;
                    outEdges.writeLong(Math.min(outcome, graph.reverseComplementEdge(outcome)));
                    outEdges.writeInt(graph.getFreg(outcome));
                    queue.add(outV);
                    wasEdges.put(Math.min(outcome,graph.reverseComplementEdge(outcome)));
                    size++;
                }
            }
            for (long income : graph.incomeEdges(cur)){
                if (!wasEdges.contains(Math.min(income,graph.reverseComplementEdge(income)))){
                    long outV = (income>>2)&graph.vertexMask;
                    outEdges.writeLong(Math.min(income, graph.reverseComplementEdge(income)));
                    outEdges.writeInt(graph.getFreg(income));
                    queue.add(outV);
                    wasEdges.put(Math.min(income,graph.reverseComplementEdge(income)));
                    size++;
                }
            }
        }
        outEdges.close();
        if ((size >= 0) && (size < COMPONENTS_MIN_SIZE)){
            (new File(fnout)).delete();
            return false;
        }
        return true;
    }

    private void buildGraph() throws IOException {
        long totalToRead = 0;
        for (File kmersFile : kmersFileFolder.get().listFiles()) {
            FileInputStream fileIn = new FileInputStream(kmersFile);
            long toRead = fileIn.getChannel().size() / 12;
            totalToRead += toRead;
            fileIn.close();
        }

        debug("have to read " + totalToRead + " k-mers");

        graphSizeBytes = Math.min(totalToRead * 24, (long)(Misc.availableMemory() * 0.85));
        debug("graph size = " + graphSizeBytes + " bytes");

        graph = new CompactDeBruijnGraphWF(k, graphSizeBytes);

        long xx = 0;
        Timer timer = new Timer();
        timer.start();
        long kmerMask = 0;
        for (File kmersFile : kmersFileFolder.get().listFiles()) {
            FileInputStream fileIn = new FileInputStream(kmersFile);
            DataInputStream in = new DataInputStream(new BufferedInputStream(fileIn));
            long toRead = fileIn.getChannel().size() / 12;
            xx += toRead;
            for (long i = 0; i < toRead; ++i) {
                long kmer = in.readLong();
                int freq = in.readInt();
                kmerMask |= kmer;
                graph.addEdge(kmer,freq);
                if (i % 1000000 == 0) {
                    System.err.println(i + " k-mers read");
                }
            }
            fileIn.close();
            debug(xx + " out of " + totalToRead + " k-mers loaded");

            double done = ((double) xx) / totalToRead;
            double total = timer.finish() / done / 1000;
            double remained = total * (1 - done);
            double elapsed = total * done;

            showProgressOnly(100 * done + "% done");
            debug("estimated  total time: " + total + ", remaining: " + remained + ", elapsed: "
                    + elapsed);
        }
        if (kmerMask != ((1L << (2 * k + 2)) - 1)) {
            warn("k-mer size mismatch");
            warn("set: " + k);
            debug(String.format("kmerMask: 0x%x", kmerMask));
            for (int i = 1; i < 30; ++i) {
                if (kmerMask == ((1L << (2 * i)) - 1)) {
                    warn("found: " + (i - 1));
                    break;
                }
            }
        }
        info("Graph size: " + graph.edgesSize());
    }

    @Override
    protected void clean() throws ExecutionFailedException {
        graph = null;

    }

    public ConnectedComponentsAssembler() {
        super(NAME, DESCRIPTION);
    }

    // ----------------------------------------------------------------------------------------------------------------
    public static void main(String[] args) {
        new ConnectedComponentsAssembler().mainImpl(args);
    }
}
