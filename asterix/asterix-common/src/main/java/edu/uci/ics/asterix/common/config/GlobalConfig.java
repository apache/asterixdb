package edu.uci.ics.asterix.common.config;

import java.util.logging.Logger;

public class GlobalConfig {

    public static final boolean DEBUG = true;
    public static final String ASTERIX_LOGGER_NAME = "edu.uci.ics.asterix";
    public static final Logger ASTERIX_LOGGER = Logger.getLogger(ASTERIX_LOGGER_NAME);
    // public static Level ASTERIX_LOG_LEVEL = Level.FINEST;

    public static final String ASTERIX_LOGFILE_PATTERN = "%t/asterix.log";
    // "%t/asterix%g.log";

    public static final String DEFAULT_CONFIG_FILE_NAME = "test.properties";

    public static final String TEST_CONFIG_FILE_NAME = "src/main/resources/test.properties";

    public static final String CONFIG_FILE_PROPERTY = "AsterixConfigFileName";

    public static final String WEB_SERVER_PORT_PROPERTY = "AsterixWebServerPort";

    public static final String BUFFER_CACHE_PAGE_SIZE_PROPERTY = "BufferCachePageSize";

    public static final String BUFFER_CACHE_SIZE_PROPERTY = "BufferCacheSize";

    public static final int DEFAULT_BUFFER_CACHE_SIZE = 4096;

    public static final String HYRACKS_APP_NAME = "asterix";

    public static final int DEFAULT_FRAME_SIZE = 32768;

    public static final String FRAME_SIZE_PROPERTY = "FrameSize";

    public static final float DEFAULT_BTREE_FILL_FACTOR = 1.00f;
    public static int DEFAULT_INPUT_DATA_COLUMN = 0;

    // static {
    // Handler h;
    // try {
    // h = new ConsoleHandler();
    // h.setFormatter(new SysoutFormatter());
    // } catch (Exception e) {
    // h = new ConsoleHandler();
    // }
    // h.setLevel(ASTERIX_LOG_LEVEL);
    // ASTERIX_LOGGER.addHandler(h);
    // ASTERIX_LOGGER.setLevel(ASTERIX_LOG_LEVEL);
    // }
}
