package ru.ifmo.genetics.statistics;

public class ExponentialDistribution {
    public final int from, direction;
    public final double variance;
    public final double lambda;
    
    public ExponentialDistribution(int from, int direction, double variance) {
        this.from = from;
        this.direction = direction;
        this.variance = variance;

        lambda = Math.pow(variance, -0.5);    // variance = lambda^(-2) for exp. distribution
    }

    public double getProb(int x) {
        x -= from;
        x *= direction;
        if (x < 0) {
            return 0;
        }
        double ans = lambda * Math.exp(-lambda * x);
        return ans;
    }
}
