package ru.ifmo.genetics.distributed.contigsJoining;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import ru.ifmo.genetics.distributed.contigsJoining.tasks.FillHoles;
import ru.ifmo.genetics.distributed.contigsJoining.tasks.FindHoles;
import ru.ifmo.genetics.distributed.contigsJoining.tasks.MakeMaybeAlignedPairs;
import ru.ifmo.genetics.distributed.util.JobUtils;

import java.io.IOException;

public class JoinContigs {
    private static Log log = LogFactory.getLog(JoinContigs.class);
    public static void main(String[] args) throws IOException {
        if (args.length != 8) {
            System.err.println("Usage: join_contigs <min-length> <max-length> <reads-path> <aligns-path> <contigs-path> <workdir> <quality-format> <right-trimming>");
            System.exit(1);
            return;
        }

        int minLength = Integer.parseInt(args[0]);
        int maxLength = Integer.parseInt(args[1]);
        Path workdir = new Path(args[2]);
        Path readsPath = new Path(args[3]);
        Path alignsPath = new Path(args[4]);
        Path contigsPath = new Path(args[5]);
        String qf = args[6];
        int trimming = Integer.parseInt(args[7]);

        log.info("work directory:\t" + workdir);
        log.info("reads directory:\t" + readsPath);
        log.info("aligns directory:\t" + alignsPath);
        log.info("contigs directory:\t" + contigsPath);

        Path joinedAlignsPath = new Path(workdir, "1_joined_aligns");
        Path holesPath = new Path(workdir, "2_holes");
        Path filledHolesPath = new Path(workdir, "3_filled_holes");



        if (!JobUtils.jobSucceededOrRemove(joinedAlignsPath)) {
            MakeMaybeAlignedPairs.make(alignsPath, readsPath, qf, joinedAlignsPath, trimming);
        }

        if (!JobUtils.jobSucceededOrRemove(holesPath)) {
            FindHoles.find(joinedAlignsPath, contigsPath, holesPath);
        }

        if (!JobUtils.jobSucceededOrRemove(filledHolesPath)) {
            FillHoles.fill(holesPath, minLength, maxLength, filledHolesPath);
        }
    }
}
