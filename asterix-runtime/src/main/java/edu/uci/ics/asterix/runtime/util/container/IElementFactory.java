package edu.uci.ics.asterix.runtime.util.container;

public interface IElementFactory<E, T> {

    public E createElement(T arg);
}
