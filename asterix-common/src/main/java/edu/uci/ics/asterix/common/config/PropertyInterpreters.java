package edu.uci.ics.asterix.common.config;

import java.util.logging.Level;

import edu.uci.ics.asterix.common.configuration.Property;

public class PropertyInterpreters {

    public static IPropertyInterpreter<Integer> getIntegerPropertyInterpreter() {
        return new IPropertyInterpreter<Integer>() {

            @Override
            public Integer interpret(Property p) throws IllegalArgumentException {
                try {
                    return Integer.parseInt(p.getValue());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(e);
                }
            }
        };
    }

    public static IPropertyInterpreter<Long> getLongPropertyInterpreter() {
        return new IPropertyInterpreter<Long>() {

            @Override
            public Long interpret(Property p) throws IllegalArgumentException {
                try {
                    return Long.parseLong(p.getValue());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(e);
                }
            }
        };
    }

    public static IPropertyInterpreter<Level> getLevelPropertyInterpreter() {
        return new IPropertyInterpreter<Level>() {

            @Override
            public Level interpret(Property p) throws IllegalArgumentException {
                return Level.parse(p.getValue());
            }
        };
    }

    public static IPropertyInterpreter<String> getStringPropertyInterpreter() {
        return new IPropertyInterpreter<String>() {

            @Override
            public String interpret(Property p) throws IllegalArgumentException {
                return p.getValue();
            }
        };
    }

    public static IPropertyInterpreter<Double> getDoublePropertyInterpreter() {
        return new IPropertyInterpreter<Double>() {

            @Override
            public Double interpret(Property p) throws IllegalArgumentException {
                try {
                    return Double.parseDouble(p.getValue());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(e);
                }
            }
        };
    }

}
