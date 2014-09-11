package ru.ifmo.genetics.utils;

import java.util.Formatter;

public class NumUtils {

    public static int signum(int x) {
        if (x < 0)
            return -1;
        if (x > 0)
            return 1;
        return 0;
    }


    public static long highestBits(long value, int bitsNumber) {
        long hb = Long.highestOneBit(value);
        long mask = 0;
        for (int shift = 0; shift < bitsNumber; shift++) {
            mask |= hb >>> shift;
        }
        return value & mask;
    }

    // returns number with at max bitsNumber highest bits that is not less than value
    public static long highestBitsUpperBound(long value, int bitsNumber) {
        long hb = Long.highestOneBit(value);
        long mask = 0;
        for (int shift = 0; shift < bitsNumber; shift++) {
            mask |= hb >>> shift;
        }
        if ((value & mask) == value) {
            return value & mask;
        }
        return (value & mask) + (hb >>> (bitsNumber - 1));
    }


    /**
     * Converts number to string with digit grouping.
     * For example, 123456789 -> 123'456'789 
     */
    public static String groupDigits(long v) {
        String vs = Long.toString(v);

        int i = vs.length() % 3;
        if (i == 0) {
            i = 3;
        }
        if (i > vs.length()) {
            i = vs.length();
        }

        String rs = vs.substring(0, i);
        while (i < vs.length()) {
            rs += "'";
            rs += vs.substring(i, i + 3);
            i += 3;
        }
        return rs;
    }
    

    /**
     * Compares prefixes of arrays.
     *
     * @param a
     * @param aLength
     * @param b
     * @param bLength
     * @return
     */
    public static boolean equals(int[] a, int aLength, int[] b, int bLength) {
        if (aLength != bLength) {
            return false;
        }

        for (int i = 0; i < aLength; ++i) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Calculate hash code for array's prefix
     *
     * @param a
     * @param aLength
     * @return
     */
    public static int hashCode(int[] a, int aLength) {
        int res = 0;

        for (int i = 0; i < aLength; ++i) {
            res *= 31;
            res += a[i];
        }
        return res;
    }

    public static void swap(int[] x, int i, int j) {
        int t = x[i];
        x[i] = x[j];
        x[j] = t;
    }

    public static void swap(short[] x, int i, int j) {
        short t = x[i];
        x[i] = x[j];
        x[j] = t;
    }

    public static int compare(long a, long b) {
        return (a < b) ? -1 : (a == b ? 0 : 1);
    }

    public static int compare(int a, int b) {
        return (a < b) ? -1 : (a == b ? 0 : 1);
    }

    public static int compare(byte a, byte b) {
        return a - b;
    }


    private static final String[] suffixes = {"", "K", "M", "G", "T", "P", "E"};
    private static final String[] formats = {"%1.2f%s", "%2.1f%s", "%3.0f%s"};
    public static String makeHumanReadable(long n) {
        Formatter f = new Formatter(new StringBuilder(5));

        if (n < 1000) {
            return n + "";
        }

        double a = n;
        int i = 0;
        while (a >= 1000) {
            a /= 1000;
            i++;
        }

        double b = a;
        int j = 0;
        while (b >= 10) {
            b /= 10;
            j++;
        }
        f.format(formats[j % 3], a, suffixes[i]);
        return f.toString();
    }
}
