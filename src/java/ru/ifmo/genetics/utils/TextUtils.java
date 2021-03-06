package ru.ifmo.genetics.utils;

import org.apache.hadoop.io.Text;

public class TextUtils {
    public static final String NL = System.getProperty("line.separator");

    public static String multiply(String s, int times) {
        StringBuilder sb = new StringBuilder(s.length() * times);
        for (int i = 0; i < times; ++i) {
            sb.append(s);
        }
        return sb.toString();
    }

    public static String multiply(char c, int length) {
        return multiply(String.valueOf(c), length);
    }
    
    
    public static String fit(String s, int len) {
        StringBuilder sb = new StringBuilder(Math.max(s.length(), len));
        sb.append(s);
        while (sb.length() < len) {
            sb.append(' ');
        }
        return sb.toString();
    }
    

    public static boolean startsWith(Text string, String start) {
        if (string.getLength() < start.length())
            return false;
        byte[] bytes = string.getBytes();
        int len = start.length();
        for (int i = 0; i < len; ++i) {
            assert start.charAt(i) <= Byte.MAX_VALUE;
            if (start.charAt(i) != bytes[i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean endsWith(Text string, String end) {
        if (string.getLength() < end.length())
            return false;
        byte[] bytes = string.getBytes();
        int endLength = end.length();
        int stringLength = string.getLength();
        for (int i = 0; i < endLength; ++i) {
            assert end.charAt(i) <= Byte.MAX_VALUE;
            if (end.charAt(i) != bytes[i + stringLength - endLength]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns position of the first whitespace after (start - 1)
     */
    public static int getWordEnd(Text string, int start) {
        byte[] bytes = string.getBytes();
        int len = string.getLength();
        int i = start;
        for (; i < len; ++i) {
            if (Character.isWhitespace(bytes[i])) {
                break;
            }
        }
        return i;

    }

    public static int parseInt(Text text, int start, int end) {
        return parseInt(text.getBytes(), start, end);
    }

    public static int parseInt(byte[] charSequence, int start, int end) {
        int res = 0;
        for (int i = start; i < end; ++i) {
            if (!('0' <= charSequence[i] && charSequence[i] <= '9')) {
                throw new NumberFormatException();
            }
            res *= 10;
            res += charSequence[i] - '0';
        }
        return res;
    }
    
    public static char getLastDigit(String s) {
        char res = 0;
        for (int i = 0; i < s.length(); ++i) {
            char cur = s.charAt(i);
            if (Character.isDigit(cur)) {
                res = cur;
            }
        }
        return res;
    }

    public static int parseInt(Text s) {
        return parseInt(s.getBytes(), 0, s.getLength());
    }
    
    
    public static boolean isYes(String s) {
        return s.equalsIgnoreCase("y") || s.equalsIgnoreCase("yes");
    }

    public static int hammingDistance(String a, String b) {
        int n = a.length();
        int m = b.length();
        int res = 0;
        for (int i = 0; i < n && i < m; ++i) {
            if (a.charAt(i) != b.charAt(i)) {
                res++;
            }
        }
        res += Math.max(n, m) - Math.min(n, m);
        return res;
    }

    public static int levenshteinDistance(String a, String b) {
        int n = a.length();
        int m = b.length();
        if (n == 0 || m == 0) {
            return (n + m);
        }

        int[][] d = new int[n + 1][m + 1];
        for (int i = 0; i <= n; ++i) {
            for (int j = 0; j <= m; ++j) {
                d[i][j] = Integer.MAX_VALUE;
            }
        }

        d[0][0] = 0;

        for (int i = 0; i <= n; ++i) {
            for (int j = 0; j <= m; ++j) {
                if (i > 0) {
                    d[i][j] = Math.min(d[i][j], d[i - 1][j] + 1);
                }
                if (j > 0) {
                    d[i][j] = Math.min(d[i][j], d[i][j - 1] + 1);
                }

                if (i > 0 && j > 0) {
                    d[i][j] = Math.min(d[i][j], d[i - 1][j - 1] + (a.charAt(i - 1) != b.charAt(j - 1) ? 1 : 0));
                }
            }
        }
        return d[n][m];
    }
}
