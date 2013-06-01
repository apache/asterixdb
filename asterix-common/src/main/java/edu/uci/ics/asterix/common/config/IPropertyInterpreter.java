package edu.uci.ics.asterix.common.config;

import edu.uci.ics.asterix.common.configuration.Property;

public interface IPropertyInterpreter<T> {
    public T interpret(Property p) throws IllegalArgumentException;
}
