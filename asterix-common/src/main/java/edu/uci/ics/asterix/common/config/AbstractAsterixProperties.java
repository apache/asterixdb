package edu.uci.ics.asterix.common.config;

public abstract class AbstractAsterixProperties {
    protected final AsterixPropertiesAccessor accessor;

    public AbstractAsterixProperties(AsterixPropertiesAccessor accessor) {
        this.accessor = accessor;
    }
}
