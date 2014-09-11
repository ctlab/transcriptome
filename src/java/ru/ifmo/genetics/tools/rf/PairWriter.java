package ru.ifmo.genetics.tools.rf;

import ru.ifmo.genetics.dna.DnaQ;
import ru.ifmo.genetics.io.IOUtils;
import ru.ifmo.genetics.utils.Misc;
import ru.ifmo.genetics.utils.pairs.UniPair;

import java.io.IOException;
import java.util.List;
import java.util.Queue;

public class PairWriter implements Runnable {
    static final int CHARS_IN_LINE = 70;

    Queue<List<UniPair<DnaQ>>> queue;
    String filePrefix;

    public PairWriter(Queue<List<UniPair<DnaQ>>> queue, String filePrefix) {
        this.queue = queue;
        this.filePrefix = filePrefix;
    }

    @Override
    public void run() {
        boolean finished = false;
        long c = 0;
        
        long tasksWritten = 0;
        boolean firstTime = true;
        while (!finished) {
            List<UniPair<DnaQ>> dnas;
            dnas = queue.poll();
            if (dnas == null) {
                try {
                    Thread.sleep(123);
                } catch (InterruptedException e) {
                    System.err.println("writing thread interrupted");
                    break;
                }
                continue;
            }

            if ((dnas.size() == 1) && (dnas.get(0) == null)) {
                break;
            }

            if (dnas.size() == 0) {
                continue;
            }

            try {
                IOUtils.dnaQs2DoubleFastaFile(Misc.extractFirsts(dnas), filePrefix + "_1", !firstTime, c, false);
                IOUtils.dnaQs2DoubleFastaFile(Misc.extractSeconds(dnas), filePrefix + "_2", !firstTime, c, false);
            } catch (IOException e) {
                System.err.println("Error while writing long reads");
                e.printStackTrace(System.err);
            }
            c += dnas.size();
            firstTime = false;

        }
    }
}
