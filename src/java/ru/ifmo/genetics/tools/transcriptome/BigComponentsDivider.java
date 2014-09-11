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

public class BigComponentsDivider extends Tool {
    public static final String NAME = "big-component-divider";
    public static final String DESCRIPTION = "divide big components";

    // input params

    public final Parameter<File> filePrefix = addParameter(new FileParameterBuilder("file-prefix")
            .mandatory()
            .withDescription("prefix of files with edges")
            .create());

    public final Parameter<Integer> kParameter = addParameter(new IntParameterBuilder("k")
            .mandatory()
            .withShortOpt("k")
            .withDescription("k-mer size (vertex, not edge)")
            .create());

    public final Parameter<File> outFilePrefix = addParameter(new FileParameterBuilder("out-file-prefix")
            .withShortOpt("po")
            .withDescription("output prefix")
            .withDefaultValue(workDir.append("/components"))
            .create());

    //constants

    private final int COMPONENTS_MIN_SIZE = 150;

    // internal vars
    private int maxFreq;
    private int k;
    private int minLenOfGen;

    int totalDeleted = 0;
    private float minNextFreq = 0.3f;

    private int toDel;

    @Override
    protected void runImpl() throws ExecutionFailedException {
        info("Big components divider started");

        k = kParameter.get();
        maxFreq = k/2 + k/4;
        minLenOfGen = k+1;
        info("Min len of gen = " + minLenOfGen);
        Timer timer = new Timer();

        info("Dividing components...");

        int prevToDel = -1;
        int cycle = 0;
        while(prevToDel != totalDeleted){
            cycle++;
            info(cycle + " cycle");
            prevToDel = totalDeleted;
            for (File in: filePrefix.get().listFiles()){
                try {
                     divideComponent(buildGraph(in), in);
                } catch (IOException e) {
                     e.printStackTrace();
                }
            }
            minNextFreq += 0.1;
        }

        info("Dividing big components done, it took " + timer);

    }

    private void divideComponent(CompactDeBruijnGraphWF graph, File in) throws IOException {
        if (graph==null ){
            return;
        }
        BigLongsHashSet wasEdges = new BigLongsHashSet(graph.getMemSize());
        makeSimple(graph,wasEdges);
        in.delete();
        totalDeleted++;
    }

    private boolean checkFrom(CompactDeBruijnGraphWF graph, BigLongsHashSet wasEdges, long curV, int dep){
        if (dep > minLenOfGen){
            return true;
        }
        if ((graph.incomeEdges(curV).length!=1) || (graph.outcomeEdges(curV).length!=1)){
            return false;
        }
        long curE = graph.outcomeEdges(curV)[0];
        if(checkFrom(graph, wasEdges, curE & graph.vertexMask, dep + 1)){
            return true;
        }
        else{
            toDel++;
            wasEdges.put(Math.min(curE,graph.reverseComplementEdge(curE)));
            return false;
        }
    }

    private void makeSimple(CompactDeBruijnGraphWF graph, BigLongsHashSet wasEdges){
        Iterator<MutableLong> iter = graph.getIterator();
        List<Long> starts = new ArrayList<Long>();
        int totalSize = 0;
        toDel = 0;
        for (MutableLong value: new IterableIterator<MutableLong>(iter)){
            long curE = value.longValue();
            long curV = curE >> 2;
            long nextV = curE & graph.vertexMask;
            totalSize++;

            if (!wasEdges.contains(Math.min(curE,graph.reverseComplementEdge(curE)))){
                double freqDiv = ((double)getFreq(curV,graph,wasEdges))/((double)getFreq(nextV,graph,wasEdges));
                if (Math.min(freqDiv,1.0/freqDiv)<minNextFreq){
                    wasEdges.put(Math.min(curE,graph.reverseComplementEdge(curE)));
                    toDel++;
                    continue;
                }

                if ((graph.incomeEdges(graph.reverseComplementEdge(curE)>>>2).length == 0)){
                    curV = graph.reverseComplementEdge(curE)>>2;
                    nextV = graph.reverseComplementEdge(curE) & graph.vertexMask;
                }

                if (graph.incomeEdges(curV).length == 0){
                    if (checkFrom(graph, wasEdges, nextV, 0)){
                        starts.add(curV);
                    }
                    else{
                        wasEdges.put(Math.min(curE,graph.reverseComplementEdge(curE)));
                        toDel++;
                    }
                }
            }
        }
        info("Prev size = " + totalSize + "; New size = " + (totalSize-toDel));
        int numOfComponents = 0;
        iter = graph.getIterator();
        for (MutableLong value: new IterableIterator<MutableLong>(iter)) {
            long curE = value.longValue();
            if (!wasEdges.contains(Math.min(curE,graph.reverseComplementEdge(curE)))){
                numOfComponents++;
                try {
                    bfs(curE&graph.vertexMask,graph,wasEdges,filePrefix.get().getAbsolutePath()+"/b"+ totalDeleted + "b"+numOfComponents);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        info("I divide big component to " + numOfComponents + " components");
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

    private boolean bfs(long start, CompactDeBruijnGraphWF graph, BigLongsHashSet wasEdges, String fnout) throws IOException {
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
                    queue.add(outV);
                    wasEdges.put(Math.min(outcome,graph.reverseComplementEdge(outcome)));
                    outEdges.writeLong(Math.min(outcome, graph.reverseComplementEdge(outcome)));
                    outEdges.writeInt(graph.getFreg(outcome));
                    size++;
                }
            }
            for (long income : graph.incomeEdges(cur)){
                if (!wasEdges.contains(Math.min(income,graph.reverseComplementEdge(income)))){
                    long outV = (income>>2)&graph.vertexMask;
                    queue.add(outV);
                    wasEdges.put(Math.min(income,graph.reverseComplementEdge(income)));
                    outEdges.writeLong(Math.min(income, graph.reverseComplementEdge(income)));
                    outEdges.writeInt(graph.getFreg(income));
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

    private CompactDeBruijnGraphWF buildGraph(File kmersFile) throws IOException {
        CompactDeBruijnGraphWF graph;

        FileInputStream fileIn = new FileInputStream(kmersFile);
        long toRead = fileIn.getChannel().size() / 12;

        fileIn.close();

        if (toRead < (1L<<20)){ //small component
            return null;
        }

        long graphSizeBytes = Math.min(toRead * 24, (long)(Misc.availableMemory() * 0.85));      //check mem

        graph = new CompactDeBruijnGraphWF(k, graphSizeBytes);


        Timer timer = new Timer();
        timer.start();
        long kmerMask = 0;

        fileIn = new FileInputStream(kmersFile);
        DataInputStream in = new DataInputStream(new BufferedInputStream(fileIn));

        for (long i = 0; i < toRead; ++i) {
            long kmer = in.readLong();
            int freq = in.readInt();
            kmerMask |= kmer;
            graph.addEdge(kmer,freq);
        }
        fileIn.close();

        info("Graph was builded");

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

        return graph;
    }

    @Override
    protected void clean() throws ExecutionFailedException {

    }

    public BigComponentsDivider() {
        super(NAME, DESCRIPTION);
    }

    // ----------------------------------------------------------------------------------------------------------------
    public static void main(String[] args) {
        new BigComponentsDivider().mainImpl(args);
    }

    private int getFreq(long v, CompactDeBruijnGraphWF graph, LongsHashSet wasEdges){
        return getFreqImpl(v,graph,wasEdges)+getFreqImpl(graph.reverseComplementEdge(v)>>2,graph,wasEdges);
    }

    private int getFreqImpl(long v, CompactDeBruijnGraphWF graph, LongsHashSet wasEdges){ //think about was (if)
        int res = 0;
        for (long outcome : graph.outcomeEdges(v)){
            if (!wasEdges.contains(Math.min(outcome,graph.reverseComplementEdge(outcome)))){
                res+=graph.getFreg(outcome);
            }
        }
        for (long income : graph.incomeEdges(v)){
            if (!wasEdges.contains(Math.min(income,graph.reverseComplementEdge(income)))){
                res+=graph.getFreg(income);
            }
        }
        return res;
    }

}

