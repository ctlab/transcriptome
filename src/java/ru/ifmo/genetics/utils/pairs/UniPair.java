package ru.ifmo.genetics.utils.pairs;

public class UniPair<D> implements Pair<D, D> {
	public final D first, second;

	public UniPair(D a, D b) {
		this.first = a;
		this.second = b;
	}
	
    @Override
    public D first() {
        return first;
    }

    @Override
    public D second() {
        return second;
    }
}
