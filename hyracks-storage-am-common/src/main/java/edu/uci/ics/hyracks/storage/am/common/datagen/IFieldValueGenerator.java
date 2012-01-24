package edu.uci.ics.hyracks.storage.am.common.datagen;

public interface IFieldValueGenerator<T> {
    public T next();
}
