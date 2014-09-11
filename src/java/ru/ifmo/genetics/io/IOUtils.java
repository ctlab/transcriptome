package ru.ifmo.genetics.io;

import org.jetbrains.annotations.NotNull;
import ru.ifmo.genetics.dna.*;
import ru.ifmo.genetics.io.formats.*;

import java.io.*;
import java.util.Iterator;

public class IOUtils {
    public static void putInt(int v, OutputStream out) throws IOException {
        out.write((v >>> 24) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write((v >>> 0) & 0xFF);
    }

    public static void putByteArray(byte[] array, OutputStream out) throws IOException {
        putInt(array.length, out);
        out.write(array);
    }

    public static int getInt(InputStream in) throws IOException {
        int ch1 = in.read();
        while (ch1 == 255) {
            ch1 = in.read();
        }
        int ch2 = in.read();
        int ch3 = in.read();
        int ch4 = in.read();
        if ((ch1 | ch2 | ch3 | ch4) < 0)
            throw new EOFException();
        return (ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0);
    }

    public static byte[] getByteArray(InputStream in) throws IOException {
        int len = getInt(in);
        byte[] b = new byte[len];
        in.read(b);
        return b;
    }

    public static void dnaQs2BinqFile(Iterable<DnaQ> dnaqs,
                                      File file) throws IOException {
        dnaQs2BinqFile(dnaqs, file.getPath());
    }

    public static void dnaQs2BinqFile(Iterable<DnaQ> dnaqs,
                                      String filename) throws IOException {
        dnaQs2BinqFile(dnaqs, filename, false);
    }

    public static void dnaQs2BinqFile(Iterator<DnaQ> dnaqsIt,
                                      String filename, boolean append) throws IOException {
        OutputStream out = new BufferedOutputStream(new FileOutputStream(filename, append));
        while (dnaqsIt.hasNext()) {
            putByteArray(dnaqsIt.next().toByteArray(), out);
        }
        out.close();
    }

    public static void dnaQs2BinqFile(Iterable<DnaQ> dnaqs,
                                      String filename, boolean append) throws IOException {
        dnaQs2BinqFile(dnaqs.iterator(), filename, append);
    }

    public static void dnaQs2FastqFile(@NotNull Iterable<DnaQ> dnaqs, String datasetName,
                                       @NotNull String filename) throws IOException {
        dnaQs2FastqFile(dnaqs, datasetName, filename, Illumina.instance);
    }

    public static void dnaQs2FastqFile(@NotNull Iterable<? extends LightDnaQ> dnaqs, String datasetName,
                                       @NotNull String filename, QualityFormat qf) throws IOException {
        dnaQs2FastqFile(dnaqs, datasetName, filename, false, 0, qf);
    }

    public static void dnaQs2FastqFile(Iterable<? extends LightDnaQ> dnaqs, String datasetName,
                                       String filename, boolean append, long startNumber, QualityFormat qf) throws IOException {
        Illumina il = new Illumina();
        PrintWriter out = new PrintWriter(new FileOutputStream(filename, append));
        long i = startNumber;
        int j = 0;
        for (LightDnaQ dnaq : dnaqs) {
            out.println("@" + datasetName + ":" + ++i + "#0/1");
            out.println(DnaTools.toString(dnaq));
            out.println("+" + datasetName + ":" + i + "#0/1");
            out.println(DnaTools.toPhredString(dnaq));
            ++j;

        }
        out.close();
    }

    public static void dnaQs2DoubleFastaFile(Iterable<DnaQ> dnaqs, String filePrefix)
            throws IOException {
        dnaQs2DoubleFastaFile(dnaqs, filePrefix, false, 0, false);
    }

    public static void dnaQs2DoubleFastaFile(Iterable<DnaQ> dnaqs,
                                             String filePrefix,
                                             boolean append, long startNumber,
                                             boolean printNs)
            throws IOException {
        PrintWriter out1 = new PrintWriter(new FileOutputStream(filePrefix + ".fasta", append));
        PrintWriter out2 = new PrintWriter(new FileOutputStream(filePrefix + ".qual", append));
        long i = startNumber;
        String baseName = filePrefix;
        int id = 1;
        if (filePrefix.endsWith("_1") || filePrefix.endsWith("_2")) {
            baseName = baseName.substring(0, baseName.length() - 2);
            id = filePrefix.endsWith("_2") ? 2 : 1;
        }
        for (DnaQ dnaq : dnaqs) {
            out1.println(">" + baseName + ":" + i + "#0/" + id);
            out2.println(">" + baseName + ":" + i + "#0/" + id);
            ++i;
            out1.println(DnaTools.toString(dnaq, printNs));
            for (int j = 0; j < dnaq.length(); ++j) {
                out2.print(dnaq.phredAt(j) + " ");
            }
            out2.println();
        }
        out1.close();
        out2.close();
    }

    public static void dnaqs2FastaFile(Iterable<? extends LightDnaQ> dnaqs,
                                       String filePrefix,
                                       boolean append, long startNumber,
                                       boolean printNs) throws FileNotFoundException {

        dnaqs2FastaFile(dnaqs, new File(filePrefix + ".fasta"), append, startNumber, printNs);
    }
    public static void dnaqs2FastaFile(Iterable<? extends LightDnaQ> dnaqs,
                                       File file,
                                       boolean append, long startNumber,
                                       boolean printNs) throws FileNotFoundException {
        PrintWriter out = new PrintWriter(new FileOutputStream(file, append));
        long i = startNumber;
        int id = 1;
        for (LightDnaQ dnaq : dnaqs) {
            out.println(">" + id);
            ++i;
            out.println(DnaTools.toString(dnaq, printNs));
        }
        out.close();
    }

    public static void dnas2FastaFile(Iterable<? extends LightDna> dnas,
                                       File file,
                                       boolean append, long startNumber) throws FileNotFoundException {
        PrintWriter out = new PrintWriter(new FileOutputStream(file, append));
        long id = startNumber;
        for (LightDna dna : dnas) {
            out.println(">" + id);
            ++id;
            out.println(DnaTools.toString(dna));
        }
        out.close();
    }

    public static void dnas2FastaFile(Iterable<? extends LightDna> dnas, File file) throws FileNotFoundException {
        dnas2FastaFile(dnas, file, false, 1);
    }

}
