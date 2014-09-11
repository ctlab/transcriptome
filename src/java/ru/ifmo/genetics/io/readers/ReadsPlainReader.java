package ru.ifmo.genetics.io.readers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import ru.ifmo.genetics.distributed.io.writable.DnaWritable;
import ru.ifmo.genetics.dna.Dna;

import java.io.*;
import java.util.ArrayList;

public class ReadsPlainReader {

    public static ArrayList<Dna> loadReads(File readsFile) throws IOException {

        Text line = new Text();

        LineReader reader = new LineReader(new BufferedInputStream(new FileInputStream(readsFile)));



        ArrayList<Dna> reads = new ArrayList<Dna>();//(100000000);
        DnaWritable tempDna = new DnaWritable();

        while (reader.readLine(line) != 0) { // skips heads
            reader.readLine(line); // reads dna
            tempDna.set(line);
            Dna dna = new Dna(tempDna);
            reads.add(dna);
        }

        reader.close();
        return reads;
    }

    public static ArrayList<Dna> loadReads(String readsFile) throws IOException {
        return loadReads(new File(readsFile));
    }
    

    public static ArrayList<Dna> loadReadsAndAddRC(File readsFile) throws IOException {

        Text line = new Text();

        LineReader reader = new LineReader(new BufferedInputStream(new FileInputStream(readsFile)));



        ArrayList<Dna> reads = new ArrayList<Dna>();//(100000000);
        DnaWritable tempDna = new DnaWritable();
        
        while (reader.readLine(line) != 0) { // skips heads
            reader.readLine(line); // reads dna
            tempDna.set(line);
            Dna dna = new Dna(tempDna);
            reads.add(dna);
            reads.add(dna.reverseComplement());
        }
        
        reader.close();
        return reads;
    }

    public static ArrayList<Dna> loadReadsAndAddRC(String readsFile) throws IOException {
        return loadReadsAndAddRC(new File(readsFile));
    }


    private static StringBuffer readLine(BufferedReader in, StringBuffer s) throws IOException {
        s.delete(0, s.length());
        while (true) {
            int c = in.read();
            if ((c == -1) || (c == '\n')) {
                break;
            }
            s.append((char)c);
        }
        return s;
    }
}
