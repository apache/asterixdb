package edu.uci.ics.asterix.runtime.util.container;

import java.util.ArrayList;
import java.util.List;

public class ListElementAllocator<E, T> implements IElementAllocator<E, T> {

    private IElementFactory<E, T> factory;
    private List<E> pool = new ArrayList<E>();
    private int cursor = -1;

    public ListElementAllocator(IElementFactory<E, T> factory) {
        this.factory = factory;
    }

    public E allocate(T arg) {
        cursor++;
        if (cursor < pool.size()) {
            return pool.get(cursor);
        } else {
            E element = factory.createElement(arg);
            pool.add(element);
            return element;
        }
    }

    public void reset() {
        cursor = -1;
    }
}
