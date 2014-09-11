package ru.ifmo.genetics.dna;

import ru.ifmo.genetics.io.formats.Illumina;
import ru.ifmo.genetics.utils.NumUtils;

import java.math.BigInteger;

public class DnaTools {

    public static final int HASH_BASE = 31;
    public static final long HASH_INVERSED_BASE;
    static {
        HASH_INVERSED_BASE = BigInteger.valueOf(HASH_BASE).modInverse(BigInteger.valueOf(2).pow(64)).longValue();
    }


    public static final long[] LONG_HASH_BASE_POWERS = new long[1024];
    static {
        long p = 1;
        for (int i = 0; i < LONG_HASH_BASE_POWERS.length; ++i) {
            LONG_HASH_BASE_POWERS[i] = p;
            p *= HASH_BASE;
        }
    }

    private DnaTools() {}

    public final static char[] NUCLEOTIDES = {'A', 'G', 'C', 'T'};
    public final static byte[] DNAcodes = {'A', 'G', 'C', 'T'};
    public final static byte[] sortedDNAcodes = {'A', 'C', 'G', 'T'};

    public static boolean isNucleotide(char c) {
        for (char nuc : NUCLEOTIDES) {
            if (c == nuc) {
                return true;
            }
        }
        return false;
    }

    public static byte fromChar(char c) {
        switch (c) {
            case 'A':
                return 0;
            case 'G':
                return 1;
            case 'C':
                return 2;
            case 'T':
                return 3;
            default:
                throw new IllegalArgumentException("Incorrect nucleotide char: \"" + c + "\"");
        }
    }

    public static char toChar(byte b) {
        return NUCLEOTIDES[b & 3];
    }

    public static long toLong(LightDna d) {
        long r = 0;
        for (int i = 0; i < d.length(); ++i) {
            r = (r << 2) + d.nucAt(i);
        }
        return r;
    }

    public static byte complement(byte b) {
        return (byte) (b ^ 3);
    }

    public static char complement(char c) {
        return toChar(complement(fromChar(c)));
    }


    /**
     * @param phred Quality score in Phred system
     * @return probability of nucleotide being correct
     */
    public static double probability(int phred) {
        return Math.max(1 - Math.pow(10, -phred * 0.1), 0.25);
    }

    public static String toString(LightDna dna) {
        char[] c = new char[dna.length()];
        for (int i = 0; i < dna.length(); i++) {
            c[i] = DnaTools.toChar(dna.nucAt(i));
        }
        return new String(c);
    }

    public static String toString(LightDnaQ dna, boolean printNs) {
        if (!printNs) {
            return toString(dna);
        }
        char[] c = new char[dna.length()];
        for (int i = 0; i < dna.length(); i++) {
            if (dna.phredAt(i) <= 2) {
                c[i] = 'N';
            } else {
                c[i] = DnaTools.toChar(dna.nucAt(i));
            }
        }
        return new String(c);
    }

    public static String toPhredString(LightDnaQ dnaq) {
        Illumina illumina = new Illumina();
        char[] c = new char[dnaq.length()];
        for (int i = 0; i < dnaq.length(); ++i) {
            c[i] = illumina.getPhredChar(dnaq.phredAt(i));
        }
        return new String(c);
    }
    
    public static boolean equals(LightDna dna1, LightDna dna2) {
        int m1 = dna1.length();
        int m2 = dna2.length();
        if (m1 != m2)
            return false;

        for (int i = 0; i < m1; ++i) {
            if (dna1.nucAt(i) != dna2.nucAt(i)) {
                return false;
            }
        }
        return true;
    }
    
    public static int hashCode(LightDna dna) {
        int length = dna.length();
        int res = 0;
        for (int i = 0; i < length; ++i) {
            res = HASH_BASE * res + dna.nucAt(i);
        }
        return res;
    }

    public static long longHashCode(LightDna dna) {
        int length = dna.length();
        long res = 0;
        for (int i = 0; i < length; ++i) {
            res = HASH_BASE * res + dna.nucAt(i);
        }
        return res;
    }

    public static int compare(LightDna dna1, LightDna dna2) {
        int m = Math.min(dna1.length(), dna2.length());
        for (int i = 0; i < m; ++i) {
            int res = NumUtils.compare(dna1.nucAt(i), dna2.nucAt(i));
            if (res != 0) {
                return res;
            }
        }

        return NumUtils.compare(dna1.length(), dna2.length());
    }

    public static boolean contains(LightDna dna, String subsequence) {
        for (int i = 0; i <= dna.length() - subsequence.length(); ++i) {
            boolean ok = true;
            for (int j = 0; j < subsequence.length(); ++j) {
                if (toChar(dna.nucAt(i + j)) != subsequence.charAt(j)) {
                    ok = false;
                    break;
                }
            }
            if (ok) {
                return true;
            }
        }
        return false;
    }
}
