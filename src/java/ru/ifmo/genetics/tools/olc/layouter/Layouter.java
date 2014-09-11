package ru.ifmo.genetics.tools.olc.layouter;

import ru.ifmo.genetics.dna.Dna;
import ru.ifmo.genetics.io.readers.ReadsPlainReader;
import ru.ifmo.genetics.tools.olc.overlaps.Overlaps;
import ru.ifmo.genetics.tools.olc.overlaps.OverlapsList;
import ru.ifmo.genetics.utils.tool.ExecutionFailedException;
import ru.ifmo.genetics.utils.tool.Parameter;
import ru.ifmo.genetics.utils.tool.Tool;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.FileParameterBuilder;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.IntParameterBuilder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;

import static ru.ifmo.genetics.tools.olc.overlaps.OverlapsList.Edge;

public class Layouter extends Tool {
    public static final String NAME = "layouter";
    public static final String DESCRIPTION = "layouts contigs";


    // input params
    public final Parameter<File> readsFile = addParameter(new FileParameterBuilder("reads-file")
            .mandatory()
            .withDescription("file with all reads")
            .create());

    public final Parameter<File> overlapsFile = addParameter(new FileParameterBuilder("overlaps-file")
            .mandatory()
            .withDescription("file with overlaps")
            .create());

    public final Parameter<File> layoutFile = addParameter(new FileParameterBuilder("layout-file")
            .optional()
            .withDefaultValue(workDir.append("layout"))
            .withDescription("file with resulting layout")
            .create());

    public final Parameter<Integer> mergeLength = addParameter(new IntParameterBuilder("merge-length")
            .optional()
            .withDefaultValue(10000)
            .withDescription("merge length")
            .create());

    public final Parameter<Integer> tipsDepth = addParameter(new IntParameterBuilder("tips-depth")
            .optional()
            .withDefaultValue(5)
            .withDescription("tips depth")
            .create());

    public final Parameter<Integer> readsNumberParameter = addParameter(new IntParameterBuilder("reads-number")
            .optional()
            .withDefaultValue(-1)
            .withDescription("strange parameter (if -1 (default), then loads all reads, " +
                    "else adds reads-number empty dna instead of reads)")
            .create());

    public final Parameter<File> finalOverlapsFile = addParameter(new FileParameterBuilder("final-overlaps-file")
            .optional()
            .withDescription("file with resulting overlaps")
            .create());

    //private String resultingOverlapsFile;

    // internal variables
    private int readsNumber;
    private ArrayList<Dna> reads;
    protected Overlaps<Dna> overlaps;
    private int averageWeight;


    @Override
    protected void runImpl() throws ExecutionFailedException {
        try {
            load();
            sortOverlaps();
            averageWeight = overlaps.getAverageWeight();

            info("Graph simplification...");
            removeTips();
            mergeGraph();
            removeTips();
//            overlaps.printToFile("overlaps.before.indel");
            mergePathsWithIndel();
//            overlaps.printToFile("overlaps.before.nonminimal");
            removeNonMinimalOverlaps();
            removeTips();
//            overlaps.printToFile("overlaps.before.indel2");
            mergePathsWithIndel();
//            overlaps.printToFile("overlaps.after.indel2");
            removeTips();
            if (finalOverlapsFile.get() != null) {
                overlaps.printToFile(finalOverlapsFile.get());
            }

            info("Dumping results...");
            boolean[] goodReads = findGoodReads();
            dumpResult(goodReads, new SimpleLayoutWriter(new PrintWriter(layoutFile.get())));

        } catch (IOException e) {
            throw new ExecutionFailedException(e);
        } catch (InterruptedException e) {
            throw new ExecutionFailedException(e);
        }
    }

    protected void load() throws IOException, InterruptedException {
        info("Loading reads...");
        readsNumber = readsNumberParameter.get();
        if (readsNumber == -1) {
            reads = ReadsPlainReader.loadReadsAndAddRC(readsFile.get().toString());
            readsNumber = reads.size();
        } else {
            Dna emptyDna = new Dna("");
            reads = new ArrayList<Dna>(readsNumber);
            for (int i = 0; i < readsNumber; ++i) {
                reads.add(emptyDna);
            }
        }

        info("Loading overlaps...");
        overlaps = new Overlaps(reads, new File[]{overlapsFile.get()}, availableProcessors.get());
    }

    protected void sortOverlaps() throws InterruptedException {
        info("Sorting overlaps...");
        overlaps.sortAll();
    }



    // ================================ remove tips ====================================

    private int removedNodes;

    protected int removeTips() {
        removedNodes = 0;
        OverlapsList tempList = new OverlapsList(overlaps.withWeights);

        for (int i = 0; i < readsNumber; ++i) {
            if (overlaps.isReadRemoved(i)) {
                continue;
            }
            if (overlaps.getForwardOverlaps(i, tempList).size() > 1) {
                for (int j = 0; j < tempList.size(); ++j) {
                    removeTipsIfAny(i, tempList.getTo(j), tipsDepth.get());
                }
            }
        }

        info(removedNodes + " nodes removed");
        return removedNodes;
    }

    /**
     * @return true if i was in tip and it was removed
     */
    private boolean removeTipsIfAny(int seed, int i, int depth) {
        if (depth <= 0) {
            return false;
        }
        if (i == seed) {
            return false;
        }
        // assert !overlaps.isReadRemoved(i) : "Removing tips from already removed read " + i + ", depth = " + depth + ", seed = " + seed;
        if (overlaps.isReadRemoved(i)) {
            return true;
        }

        OverlapsList tempList = new OverlapsList(overlaps.withWeights);
        /*
        if (overlaps.getBackwardOverlaps(i, tempList).size() > 1) {
            return false;
        }
        */

        boolean res = true;
        overlaps.removeOverlapsWithNull(i);
        overlaps.getForwardOverlaps(i, tempList);
        for (int j = 0; j < tempList.size(); ++j) {
            int to = tempList.getTo(j);
            if (!removeTipsIfAny(seed, to, depth - 1)) {
                res = false;
                return false;
            }
        }

        if (res) {
            // System.err.println("Removing node " + i + ", depth = " + depth + ", seed = " + seed);
            removeNode(i);
        }
        return res;
    }

    private void removeNode(int i) {
        if (overlaps.isReadRemoved(i)) {
            info("WARNING: removing already removed read " + i);
            return;
        }
        assert overlaps.getList(i) != null && overlaps.getList(i ^ 1) != null :
                (overlaps.getList(i) != null) + " " + (overlaps.getList(i ^ 1) != null) + " " + i;
        OverlapsList tempList = overlaps.getForwardOverlaps(i);
        for (int j = 0; j < tempList.size(); ++j) {
            int to = tempList.getTo(j);
            int shift = tempList.getCenterShift(j);
            overlaps.removeOverlap(i, to, shift);
        }

        tempList = overlaps.getBackwardOverlaps(i, tempList);
        for (int j = 0; j < tempList.size(); ++j) {
            int to = tempList.getTo(j);
            int shift = tempList.getCenterShift(j);
            overlaps.removeOverlap(to, i, -shift);
        }

        overlaps.markReadRemoved(i);
        removedNodes++;
    }



    // ================================ merge graph ======================================

    protected int merges;

    protected void mergeGraph() throws InterruptedException {
        merges = 0;

        OverlapsList tempList = new OverlapsList(overlaps.withWeights);
        int tries = 0;
        for (int i = 0; i < readsNumber; ++i) {
//            if ((i & (i - 1)) == 0) {
//                System.err.println(i + "/" + readsCount);
//            }
            if (overlaps.isReadRemoved(i)) {
                continue;
            }
            if (overlaps.getForwardOverlaps(i, tempList).size() > 1) {
                tryMergePaths(i, mergeLength.get());
                tries++;
//                if ((tries & (tries - 1)) == 0) {
//                    System.err.println(tries + " tries");
//                }
            }
        }
        info(merges + " merges");
    }

    private void tryMergePaths(int from, int depth) {
//        System.out.print("Trying to merge from vertex " + from + "... ");

        PriorityQueue<Edge> queue = new PriorityQueue<Edge>();
        HashMap<Edge, Edge> prevs = new HashMap<Edge, Edge>();
        HashSet<Integer> visited = new HashSet<Integer>();
        Edge fromEdge = new Edge(from, 0);
        queue.add(fromEdge);
        prevs.put(fromEdge, null);
        boolean firstly = true;
        int merges = 0;
        OverlapsList tempList = new OverlapsList(overlaps.withWeights);
        while (true) {
            if (!firstly && queue.size() == 1) {
                break;
            }

            if (queue.size() > 20) {
                break;
            }

            firstly = false;


            Edge u = queue.poll();

            if (u.centerShift > depth) {
                continue;
            }

            if (!visited.add(u.to)) {
                continue;
            }

            {
                int visitedSize = prevs.size();
                if ((visitedSize & 65536) != 0) {
                    break;
                }
            }

            overlaps.getForwardOverlaps(u.to, tempList);

            for (int i = 0 ; i < tempList.size(); ++i) {
                Edge v = tempList.get(i);
                v.centerShift += u.centerShift;

                if (prevs.containsKey(v)) {
                    Edge u2 = prevs.get(v);
                    if (u2 == null) {
                        continue;
                    }
                    Consensus path1consensus = getPathConsensus(fromEdge, u2, prevs);
                    Consensus path2consensus = getPathConsensus(fromEdge, u, prevs);
                    int numberOfMismatches = 0;
                    int minPositiveSize = Math.min(path1consensus.positiveSize(), path2consensus.positiveSize());
                    int minNegativeSize = Math.min(path1consensus.negativeSize(), path2consensus.negativeSize());

                    for (int j = -minNegativeSize; j < minPositiveSize; ++j) {
                        NucleotideConsensus c1 = path1consensus.getNucleotideConsensus(j);
                        NucleotideConsensus c2 = path2consensus.getNucleotideConsensus(j);
                        if (c1.size() == 0 || c2.size() == 0) {
                            continue;
                        }

                        if (c1.get() != c2.get()) {
                            numberOfMismatches++;
                        }
                    }
                    if (numberOfMismatches <= (minPositiveSize + minNegativeSize) / 10) {
                        mergeBackPaths(v, prevs.get(v), u, prevs);

                        merges++;
                    } else {
                        visited.add(v.to);
                    }
                    continue;

                }

                prevs.put(v, u);
                queue.add(v);
            }
        }
//        System.err.println(merges + " merges");
        this.merges += merges;
    }


    private Consensus getPathConsensus(Edge from, Edge to, HashMap<Edge, Edge> prevs) {
        Consensus consensus = new Consensus(overlaps.reads, 0.7, 1);
        Edge originalTo = to;
        while (to != null) {
            int shift = overlaps.centerShiftToBeginShift(from.to, to.to, to.centerShift);
            consensus.addDna(reads.get(to.to), shift);
            to = prevs.get(to);
        }
        return consensus;

    }

    private void mergeBackPaths(Edge end, Edge to1, Edge to2, HashMap<Edge, Edge> prevs) {
//        System.err.println(end + " " + to1 + " " + to2);
        // to1 and to2 are both connected to end
        if (to1.equals(to2)) {
            prevs.put(end, to1);
            return;
        }

        try {
            if (!overlaps.isWellOriented(to2.to, to1.to, to1.centerShift - to2.centerShift)) {
                Edge t = to2; to2 = to1; to1 = t;
            }
            Edge newTo2 = prevs.get(to1);
            int w;
            try {
                w = getOverlapsWeight(to2, end);
            } catch (IndexOutOfBoundsException e) {
                // :ToDo: change exception to something more specific
                // we have already removed this edge
                /**
                 *   x--->y--->z--->t--->e
                 *                      /
                 *     a--->b--->Y--->X
                 */
                // :ToDo: change handling of this situation
                w = averageWeight;
            }

            removeOverlap(to2, end);
            addOverlap(to2, to1, w);
            prevs.put(end, to1);
            mergeBackPaths(to1, to2, newTo2, prevs);

        } catch (StackOverflowError e) {
            System.err.println(end + " " + to1 + " " + to2);
            throw e;
        }
    }

    private void addOverlap(Edge from, Edge to, int weight) {
        overlaps.addOverlap(from.to, to.to, to.centerShift - from.centerShift, weight);
    }

    private int removeOverlap(Edge from, Edge to) {
        return overlaps.removeOverlap(from.to, to.to, to.centerShift - from.centerShift);
    }

    private int getOverlapsWeight(Edge from, Edge to) {
        return overlaps.getWeight(from.to, to.to, to.centerShift - from.centerShift);
    }



    // ================================== merge paths with indel ================================

    /**
     * Actualy not merges but removes one of the paths
     */
    protected void mergePathsWithIndel() {
        merges = 0;

        OverlapsList tempList = new OverlapsList(overlaps.withWeights);
        int tries = 0;
        for (int i = 0; i < readsNumber; ++i) {
            if (overlaps.isReadRemoved(i)) {
                continue;
            }
            if (overlaps.getForwardOverlaps(i, tempList).size() > 1) {
                mergePathsWithIndelStartingFrom(i, mergeLength.get());
                tries++;
            }
        }

        info(merges + " merges");
    }

    private void mergePathsWithIndelStartingFrom(int from, int depth) {
//        System.err.println("merging indels from " + from);
        PriorityQueue<Edge> queue = new PriorityQueue<Edge>();
        HashMap<Edge, Edge> prevs = new HashMap<Edge, Edge>();
        HashSet<Integer> visited = new HashSet<Integer>();
        queue.add(new Edge(from, 0));
        prevs.put(new Edge(from, 0), null);
        boolean firstly = true;
        int merges = 0;
        OverlapsList tempList = new OverlapsList(overlaps.withWeights);
        while (!queue.isEmpty()) {
            if (!firstly && queue.size() == 1) {
                break;
            }

            if (queue.size() > 20) {
                break;
            }

            firstly = false;


            Edge u = queue.poll();

            if (u.centerShift > depth) {
                continue;
            }

            if (!visited.add(u.to)) {
                continue;
            }

            overlaps.getForwardOverlaps(u.to, tempList);
//            System.err.println("going for " + u);

            for (int i = 0 ; i < tempList.size(); ++i) {
                Edge v = tempList.get(i);
                v.centerShift += u.centerShift;

                Edge v2 = new Edge(v);
                boolean removed = false;
                int maxDeviation = Math.max(4, 2 * (int)(v.centerShift * 0.01));
                for (int d = -maxDeviation; d <= maxDeviation; d += 2) {
                    v2.centerShift = v.centerShift + d;
                    if (v2.centerShift <= 0) {
                        if (prevs.containsKey(v2)) {
                            logger.warn("Short cycle found in vertex " + v2.to);
                        }
                        continue;
                    }

                    if (!prevs.containsKey(v2)) {
                        continue;
                    }

                    Edge u2 = prevs.get(v2);
                    if (overlaps.isReadRemoved(u2.to)) {
                        continue;
                    }
                    boolean pathToVIsSimple = pathIsSimple(u, prevs);
                    boolean pathToV2IsSimple = pathIsSimple(prevs.get(v2), prevs);
                    // pathToV is longer than pathToV2

                    if (pathToV2IsSimple) {
                        removePath(u2, prevs);
                        overlaps.removeOverlapsWithNull(from);
                        overlaps.removeOverlapsWithNull(v2.to ^ 1);
                        queue.remove(v2);
                        prevs.remove(v2);
                    } else if (pathToVIsSimple) {
                        removePath(u, prevs);
                        overlaps.removeOverlapsWithNull(from);
                        overlaps.removeOverlapsWithNull(v.to ^ 1);
                        removed = true;
                        break;
                    }

                }

                if (!removed) {
                    prevs.put(v, u);
                    queue.add(v);
//                    System.err.println("added " + v + " to queue");
                } else {
                    break;
                }
            }
        }
//        System.err.println(merges + " merges");
        this.merges += merges;
    }

    private void removePath(Edge to, HashMap<Edge, Edge> prevs) {
        if (overlaps.isReadRemoved(to.to)) {
            // s -> x ->y -> yrc -> xrc -> src
            // Everything before should be already removed.
            Edge prev = prevs.get(to);
            while (prev != null) {
                assert overlaps.isReadRemoved(to.to);
                to = prev;
                prev = prevs.get(to);
            }
            return;
        }
        assert !overlaps.isReadRemoved(to.to) : "Expected read " + to.to + " not to be removed";
        Edge prev = prevs.get(to);
        if (prev == null) {
            return;
        }

        overlaps.markReadRemoved(to.to);
        removePath(prev, prevs);
    }

    private boolean pathIsSimple(Edge to, HashMap<Edge, Edge> prevs) {
        assert !overlaps.isReadRemoved(to.to) : "Expected read " + to.to + " not to be removed";
        Edge prev = prevs.get(to);
        if (prev == null) {
            return true;
        }

        return overlaps.getInDegree(to.to) == 1 && overlaps.getOutDegree(to.to) == 1 && pathIsSimple(prev, prevs);
    }



    // ============================== remove non minimal overlap ===============================

    private void removeNonMinimalOverlaps() {
        Overlaps<Dna> newOverlaps = new Overlaps<Dna>(reads, availableProcessors.get(), overlaps.withWeights);

        OverlapsList tempList = new OverlapsList(overlaps.withWeights);
        for (int i = 0; i < readsNumber; ++i) {
            if (overlaps.isReadRemoved(i)) {
                newOverlaps.markReadRemoved(i);
                continue;
            }
            overlaps.removeOverlapsWithNull(i);
            overlaps.getForwardOverlaps(i, tempList);

            int maxWeight = Integer.MIN_VALUE;
            int maxJ = -1;
            for (int j = 0; j < tempList.size(); ++j) {
                int jTo = tempList.getTo(j);
                int jWeight = tempList.getWeight(j);

                if (j == 0 || jWeight >= maxWeight) {
                    maxWeight = jWeight;
                    maxJ = j;
                }
            }

            if (maxJ != -1) {
//                System.err.println("read " + i + ", adding overlap(" + i + ", " + tempList.getTo(maxJ) + ", " +
//                        tempList.getCenterShift(maxJ) + ", " + tempList.getWeight(maxJ) + ")");
                newOverlaps.addOverlap(i, tempList.getTo(maxJ), tempList.getCenterShift(maxJ), tempList.getWeight(maxJ));
            }

        }
        overlaps = newOverlaps;
    }



    // =================================== dump result =====================================

    private boolean[] findGoodReads() {
        OverlapsList tempList = new OverlapsList(overlaps.withWeights);
        boolean[] goodReads = new boolean[readsNumber];

        int badsNumber = readsNumber;
        int lonelyNumber = 0;
        for (int i =  0; i < readsNumber; ++i) {
            if (overlaps.isReadRemoved(i)) {
                continue;
            }
            int s1 = overlaps.getForwardOverlaps(i, tempList).size();
            if (s1 > 1)
                continue;

            int s2 = overlaps.getBackwardOverlaps(i, tempList).size();
            if (s2 > 1)
                continue;

            boolean lonely = (s1 == 0) && (s2 == 0);
            if (lonely) {
                lonelyNumber++;
            }
            goodReads[i] = true;
//            goodReads[i] = !lonely;
            --badsNumber;
        }

//        System.out.println("good reads found, bads - " + badsNumber);
//        System.out.println("lonely - " + lonelyNumber);
        return goodReads;
    }


    private void dumpResult(boolean[] goodReads, LayoutWriter writer)
            throws FileNotFoundException, IOException {
//        PrintWriter layoutOverlaps = new PrintWriter("overlaps.layout");

//        System.err.println("readsCount = " + readsCount);

        OverlapsList tempList = new OverlapsList(overlaps.withWeights);
        boolean[] was = new boolean[readsNumber];

        for (int i = 0; i < readsNumber; ++i) {
            if (overlaps.isReadRemoved(i) || was[i]) {
                continue;
            }
            if (!goodReads[i]) {
//                for (int j = 0; j < allOverlaps.getForwardOverlaps(i, tempList).calculateSize(); ++j) {
//                    layoutOverlaps.println(i + " " + tempList.getTo(j) + " " + tempList.getCenterShift(j));
//                }
                continue;
            }

            ArrayList<Integer> readsInCurrentContig = new ArrayList<Integer>();

//          Going back from read i
            int start = i;
            while (true) {
                overlaps.getBackwardOverlaps(start, tempList);
                if (tempList.size() == 0) {
                    break;
                }

                int newStart = tempList.getTo(0);

                if (!goodReads[newStart]) {
                    break;
                }

                start = newStart;
                if (start == i) {
                    break;
                }
            }

//          Going forward from read start
            int shift = 0;
            int cur = start;
            int end = start;
            int endShift = shift;
            while (!was[cur]) {
                writer.addLayout(cur, shift);
                readsInCurrentContig.add(cur);
                end = cur;
                endShift = shift;
                was[cur] = true;

                overlaps.getForwardOverlaps(cur, tempList);

                if (tempList.size() == 0) {
                    break;
                }

                int newCur = tempList.getTo(0);

                if (!goodReads[newCur]) {
                    break;
                }

                shift += overlaps.centerShiftToBeginShift(cur, tempList.getTo(0), tempList.getCenterShift(0));
                cur = newCur;
            }
//            if (allOverlaps.getForwardOverlaps(end, tempList).calculateSize() == 0) {
//                if (allOverlaps.getBackwardOverlaps(start, tempList).calculateSize() != 0)
//                layoutOverlaps.println(start + " " + end + " " + endShift);
//            }
//            allOverlaps.getForwardOverlaps(end, tempList);
//            for (int j = 0; j < tempList.calculateSize(); ++j) {
//                layoutOverlaps.println(start + " " + tempList.getTo(j) + " " + (tempList.getCenterShift(j) + endShift));
//            }
            writer.flush();
            for (int r: readsInCurrentContig) {
                was[r ^ 1] = true;
            }
        }

        for (int i = 0; i < readsNumber; ++i) {
            if (overlaps.isReadRemoved(i)) {
                continue;
            }
            if (was[i]) {
                continue;
            }
            writer.addLayout(i, 0);
            writer.flush();
            was[i] = true;
            was[i ^ 1] = true;
        }
        writer.close();
//        layoutOverlaps.close();
    }




    @Override
    protected void clean() throws ExecutionFailedException {
        reads = null;
        overlaps = null;
    }

    public Layouter() {
        super(NAME, DESCRIPTION);
    }

}
