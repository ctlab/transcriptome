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

public class SmallComponentsAssembler extends Tool {
    public static final String NAME = "small-components-assembler";
    public static final String DESCRIPTION = "assembles obvious transcripts";

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

    //constants
    private final float minNextFreq = 0.1f;

    // internal vars
    private int k;
    private int minLenOfGen;

    volatile Integer total = 0;

    int totalTranscripts = 0;

    @Override
    protected void runImpl() throws ExecutionFailedException {

        k = kParameter.get();
        minLenOfGen = (k+1);
        info("Min len of gen = " + minLenOfGen);
        Timer timer = new Timer();

        info("Assembling transcripts...");
        final File outDir = new File(workDir.get().getAbsolutePath() + "/transcripts");
        outDir.mkdir();

        File [] allComponentFiles = filePrefix.get().listFiles();

        int totalThreads = availableProcessors.get();

        int bagLength = allComponentFiles.length / totalThreads;

        Thread []allThreads = new Thread[totalThreads];

        for (int threadNum = 0; threadNum < totalThreads; threadNum++){
            int startIndex = threadNum * bagLength;
            int endIndex = startIndex + bagLength;
            if (threadNum == totalThreads - 1){
                endIndex = allComponentFiles.length;
            }
            final File []componentFilesBag = Arrays.copyOfRange(allComponentFiles,startIndex,endIndex);
            allThreads[threadNum] = new Thread(){
                public void run(){
                    for (File in: componentFilesBag){
                        try {
                            assebleTranscripts(buildGraph(in),new File(outDir.getAbsolutePath()+ "/"+in.getName()));
                            synchronized (total){
                                total++;
                                if (total%1000 == 0){
                                    info(total + " components analyzed");
                                }
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            };
            allThreads[threadNum].start();
            info("Thread started " + threadNum);

        }
        for (int threadNum = 0; threadNum < totalThreads; threadNum++){
            try {
                allThreads[threadNum].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        info("Assembling transcripts done, it took " + timer);
    }

    private void assebleTranscripts(CompactDeBruijnGraphWF graph, File out) throws IOException {
        if (graph==null ){
            return;
        }
        BigLongsHashSet wasEdges = new BigLongsHashSet(graph.getMemSize());
        List<Long> starts = makeSimple(graph,wasEdges);
        if (starts.size() == 0){
            return;
        }
        else{
            getTranscripts(starts,out.getAbsolutePath(),graph,wasEdges);
        }
    }


    private boolean dfs(long start, PrintWriter out, CompactDeBruijnGraphWF graph,LongsHashSet wasEdges,Set<Long> wasStrats){
        Stack<Long> dfsStack = new Stack<Long>();
        Stack<Integer> dfsStringLen = new Stack<Integer>();
        dfsStack.push(start);
        dfsStringLen.push(0);
        StringBuilder curTs = new StringBuilder();
        int curLen = 0;
        int lenGen = 0;
        int numOfTr = 1;
        Map<Long,Integer> wasVerts = new HashMap<Long, Integer>();
        while (!dfsStack.isEmpty()){
            long curV = dfsStack.pop();
            int nextLen = dfsStringLen.pop();
            wasStrats.add(Math.min(curV,graph.reverseComplementEdge(curV)>>2));

            if (curLen != nextLen){
                curTs.delete(nextLen,curTs.length());
                lenGen = 0;
                curLen = nextLen;
                numOfTr++;
            }

            curTs.append(KmerUtils.kmer2String(curV,k).charAt(0));
            curLen++;
            lenGen++;
            wasVerts.put(curV,numOfTr);

            int outEdgesCount = 0;
            for (long outcome: graph.outcomeEdges(curV)){
                if (!wasEdges.contains(Math.min(outcome,graph.reverseComplementEdge(outcome))) && (!wasVerts.containsKey(outcome&graph.vertexMask) || (wasVerts.get(outcome&graph.vertexMask)<numOfTr))){
                    float freqRatio = (float)(getFreq(curV,graph,wasEdges))/(float)(getFreq(outcome&graph.vertexMask,graph,wasEdges));
                    if (Math.min(freqRatio,1f/freqRatio)>minNextFreq){
                        outEdgesCount++;
                    }
                }
            }
            if (outEdgesCount > 1){
                lenGen = 0;
                wasVerts.put(curV,numOfTr);
            }

            for (long outcome: graph.outcomeEdges(curV)){
                if (!wasEdges.contains(Math.min(outcome,graph.reverseComplementEdge(outcome)))&& (!wasVerts.containsKey(outcome&graph.vertexMask) || (wasVerts.get(outcome&graph.vertexMask)<numOfTr))){
                    float freqRatio = (float)(getFreq(curV,graph,wasEdges))/(float)(getFreq(outcome&graph.vertexMask,graph,wasEdges));
                    if (Math.min(freqRatio,1f/freqRatio)>minNextFreq){
                        dfsStack.push(outcome&graph.vertexMask);
                        dfsStringLen.push(curLen);
                    }
                }
            }
            if ((outEdgesCount == 0) && (lenGen>= minLenOfGen)){
                totalTranscripts++;
                out.println(">" + totalTranscripts + " len="+curTs.length());
                out.println(curTs+KmerUtils.kmer2String(curV,k).substring(1));
            }

            if (numOfTr > 50){
                return false;
            }
        }
        return true;
    }

    private void getTranscripts(List<Long> starts, String fnout, CompactDeBruijnGraphWF graph,LongsHashSet wasEdges) throws IOException {
        File fout = new File(fnout+"tr.fasta");
        fout.createNewFile();
        PrintWriter out = new PrintWriter(fout);
        Set<Long> wasStarts = new HashSet<Long>();
        for (long start: starts){
            if (!wasStarts.contains(Math.min(start,graph.reverseComplementEdge(start)>>2))){
                if (!dfs(start,out,graph,wasEdges,wasStarts)){
                    out.close();
                    fout.delete();
                    return;
                }
            }
        }
        out.close();
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
            wasEdges.put(Math.min(curE,graph.reverseComplementEdge(curE)));
            return false;
        }
    }

    private List<Long> makeSimple(CompactDeBruijnGraphWF graph, BigLongsHashSet wasEdges){
        Iterator<MutableLong> iter = graph.getIterator();
        List<Long> starts = new ArrayList<Long>();
        for (MutableLong value: new IterableIterator<MutableLong>(iter)){
            long curE = value.longValue();
            long curV = curE >> 2;
            long nextV = curE & graph.vertexMask;

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
                }
            }

        }
        return starts;
    }

    private CompactDeBruijnGraphWF buildGraph(File kmersFile) throws IOException {
        CompactDeBruijnGraphWF graph;

        FileInputStream fileIn = new FileInputStream(kmersFile);
        long toRead = fileIn.getChannel().size() / 12;

        fileIn.close();

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

    public SmallComponentsAssembler() {
        super(NAME, DESCRIPTION);
    }

    // ----------------------------------------------------------------------------------------------------------------
    public static void main(String[] args) {
        new SmallComponentsAssembler().mainImpl(args);
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

