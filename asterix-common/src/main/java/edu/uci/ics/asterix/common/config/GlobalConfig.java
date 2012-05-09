package edu.uci.ics.asterix.common.config;

import java.util.logging.Logger;

public class GlobalConfig {
    public static final boolean DEBUG = true;
    
    public static final String ASTERIX_LOGGER_NAME = "edu.uci.ics.asterix";
    
    public static final Logger ASTERIX_LOGGER = Logger.getLogger(ASTERIX_LOGGER_NAME);

    public static final String ASTERIX_LOGFILE_PATTERN = "%t/asterix.log";

    public static final String DEFAULT_CONFIG_FILE_NAME = "test.properties";

    public static final String TEST_CONFIG_FILE_NAME = "src/main/resources/test.properties";

    public static final String CONFIG_FILE_PROPERTY = "AsterixConfigFileName";

    public static final String WEB_SERVER_PORT_PROPERTY = "AsterixWebServerPort";

    public static final String BUFFER_CACHE_PAGE_SIZE_PROPERTY = "BufferCachePageSize";

    public static final String BUFFER_CACHE_NUM_PAGES_PROPERTY = "BufferCacheNumPages";

    public static final int DEFAULT_BUFFER_CACHE_NUM_PAGES = 4096;

    public static final String HYRACKS_APP_NAME = "asterix";

    public static final int DEFAULT_FRAME_SIZE = 32768;

    public static final String FRAME_SIZE_PROPERTY = "FrameSize";

    public static final float DEFAULT_BTREE_FILL_FACTOR = 1.00f;
    
    public static int DEFAULT_INPUT_DATA_COLUMN = 0;
}
