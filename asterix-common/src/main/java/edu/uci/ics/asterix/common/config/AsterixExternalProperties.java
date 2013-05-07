package edu.uci.ics.asterix.common.config;

import java.util.logging.Level;

public class AsterixExternalProperties extends AbstractAsterixProperties {

    private static final String EXTERNAL_WEBPORT_KEY = "external.webport";
    private static int EXTERNAL_WEBPORT_DEFAULT = 19001;

    private static final String EXTERNAL_LOGLEVEL_KEY = "external.loglevel";
    private static String EXTERNAL_LOGLEVEL_DEFAULT = "INFO";

    public AsterixExternalProperties(AsterixPropertiesAccessor accessor) {
        super(accessor);
    }

    public int getWebInterfacePort() {
        return accessor.getInt(EXTERNAL_WEBPORT_KEY, EXTERNAL_WEBPORT_DEFAULT);
    }

    public Level getLogLevel() {
        String level = accessor.getString(EXTERNAL_LOGLEVEL_KEY, EXTERNAL_LOGLEVEL_DEFAULT);
        return Level.parse(level);
    }

}
