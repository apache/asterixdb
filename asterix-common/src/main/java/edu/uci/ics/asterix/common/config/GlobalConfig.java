/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.common.config;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class GlobalConfig {
    public static final boolean DEBUG = true;

    public static final String ASTERIX_LOGGER_NAME = "edu.uci.ics.asterix";

    public static final Logger ASTERIX_LOGGER = Logger.getLogger(ASTERIX_LOGGER_NAME);

    public static final String DEFAULT_CONFIG_FILE_NAME = "asterix-configuration.xml";

    public static final String CONFIG_FILE_PROPERTY = "AsterixConfigFileName";

    public static final String WEB_SERVER_PORT_PROPERTY = "AsterixWebServerPort";

    public static final String JSON_API_SERVER_PORT_PROPERTY = "AsterixJSONAPIServerPort";

    public static final int DEFAULT_FRAME_SIZE = 32768;

    public static final String FRAME_SIZE_PROPERTY = "FrameSize";

    public static final float DEFAULT_BTREE_FILL_FACTOR = 1.00f;

    public static int DEFAULT_INPUT_DATA_COLUMN = 0;

    public static final String DEFAULT_COMPACTION_POLICY_NAME = "prefix";

    public static final Map<String, String> DEFAULT_COMPACTION_POLICY_PROPERTIES;
    static {
        DEFAULT_COMPACTION_POLICY_PROPERTIES = new HashMap<String, String>();
        DEFAULT_COMPACTION_POLICY_PROPERTIES.put("max-mergable-component-size", "1073741824"); // 1GB
        DEFAULT_COMPACTION_POLICY_PROPERTIES.put("max-tolernace-component-count", "5"); // 5 components
    }

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
