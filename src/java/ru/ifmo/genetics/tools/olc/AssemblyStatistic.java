package ru.ifmo.genetics.tools.olc;

import ru.ifmo.genetics.dna.Dna;
import ru.ifmo.genetics.io.readers.ReadsPlainReader;
import ru.ifmo.genetics.utils.Misc;
import ru.ifmo.genetics.utils.tool.ExecutionFailedException;
import ru.ifmo.genetics.utils.tool.Parameter;
import ru.ifmo.genetics.utils.tool.Tool;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.FileParameterBuilder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.TreeMap;

import static ru.ifmo.genetics.utils.NumUtils.groupDigits;

public class AssemblyStatistic extends Tool {
    public static final String NAME = "assembly-statistic";
    public static final String DESCRIPTION = "calculates simply statistics on assembled contigs";


    // input parameters
    public final Parameter<File> readsFile = addParameter(new FileParameterBuilder("reads-file")
            .mandatory()
            .withDescription("file with all reads")
            .create());


    // internal variables
    private TreeMap<Long, Integer> lengths = new TreeMap<Long, Integer>(new Comparator<Long>() {
        @Override
        public int compare(Long o1, Long o2) {
            return o1 < o2 ? 1 : o1 > o2 ? -1 : 0;
        }
    });
    private int contigsNumber = 0;
    private long totalLength = 0;
    private long[] n = new long[101];

    private int readsNumber;
    private ArrayList<Dna> reads;


    @Override
    protected void runImpl() throws ExecutionFailedException {
        try {
            loadReads();
            
            info("Calculating...");
            for (Dna dna : reads) {
                add(dna.length());
            }
            
            info("Statistic:\n" + toString());
        } catch (IOException e) {
            throw new ExecutionFailedException(e);
        } catch (InterruptedException e) {
            throw new ExecutionFailedException(e);
        }
    }

    private void loadReads() throws IOException, InterruptedException {
        info("Loading reads...");
        reads = ReadsPlainReader.loadReads(readsFile.get().toString());
        readsNumber = reads.size();
    }


    private void add(long length) {
        if (length == 0) {
            warn("What the fuck are you doing here, damned zero-length contig?!!");
            return;
        }
        Misc.incrementInt(lengths, length);
        totalLength += length;
        ++contigsNumber;
    }

    private long getContigsNumber() {
        return contigsNumber;
    }

    private long getN(int percent) {
        return n[percent];
    }
    
    private long getMaxLength() {
        return lengths.isEmpty() ? 0 : lengths.firstKey();
    }

    private long getMinLength() {
        return lengths.isEmpty() ? 0 : lengths.lastKey();
    }

    private long getTotalLength() {
        return totalLength;
    }

    private long getMeanLength() {
        return lengths.isEmpty() ? 0 : Math.round(((double)totalLength) / contigsNumber);
    }
    
    private void update() {
        int p = 0;
        long curLength = 0;
        for (long length : lengths.keySet()) {
            curLength += length * lengths.get(length);
            int percent = (int)(curLength * 100 / totalLength);
            for (int i = p; i <= percent; ++i) {
                n[i] = length;
            }
            p = percent + 1;
        }
    }
    
    @Override
    public String toString() {
        update();

        StringBuilder sb = new StringBuilder();
        sb.append("total contigs: " + groupDigits(getContigsNumber()) + "\n");
        sb.append("total length: " + groupDigits(getTotalLength()) + "\n");
        sb.append("maximal length: " + groupDigits(getMaxLength()) + "\n");
        sb.append("mean length: " + groupDigits(getMeanLength()) + "\n");
        sb.append("minimal length: " + groupDigits(getMinLength()) + "\n");
        sb.append("n50: " + groupDigits(getN(50)) + "\n");
        sb.append("n90: " + groupDigits(getN(90)) + "\n");
//        sb.append("lens: " + lengths);
        
        return sb.toString();
    }


    @Override
    protected void clean() throws ExecutionFailedException {
        lengths = null;
        n = null;
        reads = null;
    }

    public AssemblyStatistic() {
        super(NAME, DESCRIPTION);
    }

    public static void main(String[] args) {
        new AssemblyStatistic().mainImpl(args);
    }
}
