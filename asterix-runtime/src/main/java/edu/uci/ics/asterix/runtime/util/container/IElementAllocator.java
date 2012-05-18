package edu.uci.ics.asterix.runtime.util.container;

public interface IElementAllocator<E, T> {

    public E allocate(T arg);

    public void reset();
}
