package ru.ifmo.genetics.statistics.reporter;


import org.apache.log4j.Logger;

public class LocalMonitor implements Runnable {
    private Logger log;
    private Report report;
    private final static int INITIAL_INTERVAL_MS = 1000;
    private final static int MAXIMAL_INTERVAL_MS = 60000;
    private String oldReport = null;

    public LocalMonitor(Report report) {
        log = Logger.getLogger(report.name());
        this.report = report;
    }

    @Override
    public void run() {
        int waitTime = INITIAL_INTERVAL_MS;
        while (true) {
            try {
                Thread.sleep(waitTime);
            }  catch (InterruptedException e) {
                printReport();
                break;
            }
            printReport();
            waitTime = Math.min(waitTime * 2, MAXIMAL_INTERVAL_MS);
        }
    }

    private void printReport() {
        String currentReport = report.toString();
        if (currentReport == null || currentReport.equals("")) {
            return;
        }

        if (currentReport.equals(oldReport)) {
            return;
        }
        log.debug(currentReport);
        oldReport = currentReport;
    }

    private Thread thread = null;

    public void start() {
        if (thread != null) {
            throw new IllegalStateException("Starting already running monitor");
        }
        thread = new Thread(this);
        thread.start();
    }

    public void stop() {
        thread.interrupt();
        printReport();
        thread = null;
    }
}
