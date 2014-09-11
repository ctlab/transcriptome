package ru.ifmo.genetics.statistics;

public class Timer {
    private long time;

    public Timer() {
        start();
    }

    public void start() {
        time = System.currentTimeMillis();
    }

    public long finish() {
        return System.currentTimeMillis() - time;
    }
    
    public static String timeToString(long timeMillis) {
        long ms = timeMillis % 1000;
        long s = timeMillis / 1000;
        if (s == 0) {
            return ms + " ms";
        }
        
        long m = s / 60;
        s %= 60;
        if (m == 0) {
            return s + " s " + ms + " ms";
        }

        long h = m / 60;
        m %= 60;
        if (h == 0) {
            return m + " min " + s + " s";
        }
        
        long d = h / 24;
        h %= 24;
        if (d == 0) {
            return h + " h " + m + " min";
        }
        
        return d + " day(s) " + h + " h";
    }

    public static String timeToStringWithoutMs(long timeMillis) {
        long s = Math.round(timeMillis / 1000.0);
        long m = s / 60;
        s %= 60;
        if (m == 0) {
            return s + " s";
        }

        long h = m / 60;
        m %= 60;
        if (h == 0) {
            return m + " min " + s + " s";
        }

        long d = h / 24;
        h %= 24;
        if (d == 0) {
            return h + " h " + m + " min";
        }

        return d + " day(s) " + h + " h";
    }

    @Override
    public String toString() {
        long time = finish();
        return timeToString(time);
    }
    
}
