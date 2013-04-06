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

    public static final String JSON_API_SERVER_PORT_PROPERTY = "AsterixJSONAPIServerPort";

    public static final String BUFFER_CACHE_PAGE_SIZE_PROPERTY = "BufferCachePageSize";

    public static final String BUFFER_CACHE_NUM_PAGES_PROPERTY = "BufferCacheNumPages";

    public static final int DEFAULT_BUFFER_CACHE_NUM_PAGES = 4096;

    public static final int DEFAULT_FRAME_SIZE = 32768;

    public static final String FRAME_SIZE_PROPERTY = "FrameSize";

    public static final float DEFAULT_BTREE_FILL_FACTOR = 1.00f;

    public static int DEFAULT_INPUT_DATA_COLUMN = 0;

    public static int DEFAULT_INDEX_MEM_PAGE_SIZE = 32768;

    public static int DEFAULT_INDEX_MEM_NUM_PAGES = 1000;

    public static int getFrameSize() {
        int frameSize = GlobalConfig.DEFAULT_FRAME_SIZE;
        String frameSizeStr = System.getProperty(GlobalConfig.FRAME_SIZE_PROPERTY);
        if (frameSizeStr != null) {
            int fz = -1;
            try {
                fz = Integer.parseInt(frameSizeStr);
            } catch (NumberFormatException nfe) {
                GlobalConfig.ASTERIX_LOGGER.warning("Wrong frame size size argument. Picking default value ("
                        + GlobalConfig.DEFAULT_FRAME_SIZE + ") instead.\n");
            }
            if (fz >= 0) {
                frameSize = fz;
            }
        }
        return frameSize;
    }
}
