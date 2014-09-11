package ru.ifmo.genetics.tools.olc.layouter;

import org.apache.commons.cli.*;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import ru.ifmo.genetics.dna.Dna;
import ru.ifmo.genetics.io.readers.ReadsPlainReader;
import ru.ifmo.genetics.tools.olc.overlaps.Overlaps;
import ru.ifmo.genetics.tools.olc.overlaps.OverlapsList;
import ru.ifmo.genetics.utils.Misc;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;


public class NeighbourhoodPrinter {
    private String readsFile;
    private String overlapsFile;
    private String outputFile;
    private List<Integer> centers;
    private int depth;
    private int readsNumber = -1;
    private Configuration config;

    public NeighbourhoodPrinter(Configuration config) {
        depth = config.getInt("depth");
        overlapsFile = config.getString("overlaps");
        outputFile = config.getString("output");
        readsFile = config.getString("reads");
        centers = new ArrayList<Integer>();
        readsNumber = config.getInt("reads_number", -1);
        for (Object s: config.getList("centers")) {
            centers.add(Integer.parseInt((String)s));
        }
        this.config = config;
    }

    public void run() throws IOException, InterruptedException {
        ArrayList<Dna> reads;
        if (readsNumber == -1) {
            reads = ReadsPlainReader.loadReadsAndAddRC(readsFile);
            readsNumber = reads.size();
        } else {
            reads = new ArrayList<Dna>(readsNumber);
            for (int i = 0; i < readsNumber; ++i) {
                reads.add(Dna.emptyDna);
            }
        }

        Overlaps<Dna> allOverlaps = new Overlaps<Dna>(reads, new File[]{new File(overlapsFile)}, 6);
        System.err.println("overlaps loaded");


        if (centers.contains(-1)) {
            centers.clear();
            for (int i = 0; i < reads.size(); i++) {
                centers.add(i);
            }
        }

        HashSet<Integer> added = new HashSet<Integer>();
        added.addAll(centers);
        HashSet<Integer> queue = new HashSet<Integer>();
        queue.addAll(centers);
        PrintWriter out = new PrintWriter(outputFile);

        OverlapsList edges = new OverlapsList(allOverlaps.withWeights);
        for (int i = 0; i < depth; ++i) {
            HashSet<Integer> nextQueue = new HashSet<Integer>();

            for (int v : queue) {
                allOverlaps.getAllOverlaps(v, edges);
                for (int j = 0; j < edges.size(); ++j) {
                    int vv = v;
                    int nv = edges.getTo(j);
                    int centerShift = edges.getCenterShift(j);

                    if (!added.contains(nv)) {
                        added.add(nv);
                        nextQueue.add(nv);
                    }

                    if (queue.contains(nv) || nextQueue.contains(nv)) {
                        if (!Overlaps.isWellOriented(vv, nv, centerShift)) {
                            int t = vv;
                            vv = nv;
                            nv = t;
                            centerShift = -centerShift;
                        }

                        out.print(vv + " " + nv + " " + centerShift);
                        if (edges.isWithWeights()) {
                            out.println(" " + edges.getWeight(j));
                        } else {
                            out.println();
                        }
                    }
                }
            }

            queue = nextQueue;
        }


        int S = 2000000000;
        int T = 2000000001;

        for (int v: queue) {
            allOverlaps.getAllOverlaps(v, edges);

            boolean hasNotVisitedOutEdge = false;
            boolean hasNotVisitedInEdge = false;

            for (int j = 0; j < edges.size(); ++j) {
                int nv = edges.getTo(j);
                int centerShift = edges.getCenterShift(j);
                if (!added.contains(nv)) {
                    if (Overlaps.isWellOriented(v, nv, centerShift)) {
                        hasNotVisitedOutEdge = true;
                    } else {
                        hasNotVisitedInEdge = true;
                    }
                }
            }

            if (hasNotVisitedOutEdge) {
                out.println(v + " " + T + " " + 0);
            }
            if (hasNotVisitedInEdge) {
                out.println(S + " " + v + " " + 0);
            }
        }


        System.err.println("Reachable vertex count: " + added.size());

        out.close();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Options options = new Options();

        options.addOption("h", "help", false, "prints this message");
        options.addOption("o", "output", true,  "sets the ouput file name");
        options.addOption("O", "overlaps", true,  "sets the overlaps file name");
        options.addOption("d", "depth", true,  "sets the depth of neighbourhood");
        options.addOption("r", "reads", true,  "sets the reads file name");
        options.addOption("n", "reads-number", true,  "sets the number of reads");
        options.addOption("C", "centers", true,  "sets the center of neighbourhood");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            e.printStackTrace(System.err);
            return;
        }

        if (cmd.hasOption("help")) {
            new HelpFormatter().printHelp("overlap", options);
            return;
        }

        Configuration config = new PropertiesConfiguration();

        Misc.addOptionToConfig(cmd, config, "output");
        Misc.addOptionToConfig(cmd, config, "overlaps");
        Misc.addOptionToConfig(cmd, config, "centers");
        Misc.addOptionToConfig(cmd, config, "depth");
        Misc.addOptionToConfig(cmd, config, "reads");
        Misc.addOptionToConfig(cmd, config, "reads-number");

        new NeighbourhoodPrinter(config).run();
    }
}
