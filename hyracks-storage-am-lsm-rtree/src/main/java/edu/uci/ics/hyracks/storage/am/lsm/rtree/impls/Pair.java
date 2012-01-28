package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

public class Pair<A, B> {
    private final A first;
    private final B second;

    public Pair(A first, B second) {
        super();
        this.first = first;
        this.second = second;
    }

    public A getFirst() {
        return first;
    }

    public B getSecond() {
        return second;
    }
}