package edu.uci.ics.hyracks.storage.am.lsmtree.datagen;

public interface IFieldValueGenerator<T> {
    public T next();
}
