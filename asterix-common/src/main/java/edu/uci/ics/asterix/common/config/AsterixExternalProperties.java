package edu.uci.ics.asterix.common.config;

import java.util.logging.Level;

public class AsterixExternalProperties extends AbstractAsterixProperties {

    private static final String EXTERNAL_WEBPORT_KEY = "external.webport";
    private static int EXTERNAL_WEBPORT_DEFAULT = 19001;

    private static final String EXTERNAL_LOGLEVEL_KEY = "external.loglevel";
    private static Level EXTERNAL_LOGLEVEL_DEFAULT = Level.INFO;

    private static final String EXTERNAL_APISERVER_KEY = "external.apiserver";
    private static int EXTERNAL_APISERVER_DEFAULT = 19101;

    public AsterixExternalProperties(AsterixPropertiesAccessor accessor) {
        super(accessor);
    }

    public int getWebInterfacePort() {
        return accessor.getProperty(EXTERNAL_WEBPORT_KEY, EXTERNAL_WEBPORT_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public int getAPIServerPort() {
        return accessor.getProperty(EXTERNAL_APISERVER_KEY, EXTERNAL_APISERVER_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public Level getLogLevel() {
        return accessor.getProperty(EXTERNAL_LOGLEVEL_KEY, EXTERNAL_LOGLEVEL_DEFAULT,
                PropertyInterpreters.getLevelPropertyInterpreter());
    }
}
