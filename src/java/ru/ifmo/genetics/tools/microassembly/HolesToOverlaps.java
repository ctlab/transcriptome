package ru.ifmo.genetics.tools.microassembly;

import org.apache.commons.cli.*;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import ru.ifmo.genetics.dna.Dna;
import ru.ifmo.genetics.io.readers.ReadsPlainReader;
import ru.ifmo.genetics.tools.olc.overlaps.Overlaps;
import ru.ifmo.genetics.utils.Misc;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;

public class HolesToOverlaps {
    String holesFile;
    String overlapsOutputFile;
    String readsOutputFile;
    String readsFile;
    Configuration config;

    public static int stringIdToInt(String id) {
        if (id.equals("" + Integer.MAX_VALUE)) {
            return Integer.MAX_VALUE;
        }
        if (id.endsWith("rc")) {
            return Integer.parseInt(id.substring(0, id.length() - 2)) * 2 + 1;
        } else {
            return Integer.parseInt(id) * 2;
        }
    }

    public HolesToOverlaps(Configuration config) {
        holesFile = config.getString("holes");
        overlapsOutputFile = config.getString("overlaps_output");
        readsFile = config.getString("reads");
        this.config = config;
    }

    public static void main(String[] args) throws IOException, InterruptedException, ConfigurationException {
        Options options = new Options();

        options.addOption("h", "help", false, "prints this message");
        options.addOption("c", "config", true,  "sets the config file name, default to config.properties");
        options.addOption("o", "overlaps-output", true,  "sets the ouput file name");
        options.addOption("H", "holes", true,  "sets the holes file name");
        options.addOption("r", "reads", true,  "sets the reads file name");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            e.printStackTrace(System.err);
            return;
        }

        if (cmd.hasOption("help")) {
            new HelpFormatter().printHelp("holes2overlaps", options);
            return;
        }

        String configFileName = cmd.getOptionValue("config", "config.properties");
        Configuration config = new PropertiesConfiguration(configFileName);

        Misc.addOptionToConfig(cmd, config, "overlaps-output");
        Misc.addOptionToConfig(cmd, config, "reads");
        Misc.addOptionToConfig(cmd, config, "holes");

        new HolesToOverlaps(config).run();
    }

    private void run() throws IOException {
        ArrayList<Dna> reads = ReadsPlainReader.loadReadsAndAddRC(readsFile);

        Overlaps<Dna> overlaps = new Overlaps<Dna>(reads, 6, true);
        Text line = new Text();

        LineReader holesReader = new LineReader(new BufferedInputStream(new FileInputStream(holesFile)));

        while (holesReader.readLine(line) != 0) {
            String s = line.toString();
            FilledHole fh = new FilledHole(s);

            if (fh.hole.isOpen()) {
                continue;
            }

            int iFrom = fh.hole.leftContigId * 2 + (fh.hole.leftComplemented ? 1 : 0);
            int iTo = fh.hole.rightContigId * 2 + (fh.hole.rightComplemented ? 1 : 0);


            int shift = reads.get(iFrom).length() + fh.filler.distance;
            overlaps.addRawOverlap(iFrom, iTo,
                    shift, fh.filler.weight);
        }
        holesReader.close();


        overlaps.printToFile(overlapsOutputFile);

    }
}
