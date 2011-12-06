/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.algebricks.core.config;

import java.util.logging.Logger;

public class AlgebricksConfig {
    public static final boolean DEBUG = true;
    public static final String ALGEBRICKS_LOGGER_NAME = "edu.uci.ics.algebricks";
    public static final Logger ALGEBRICKS_LOGGER = Logger.getLogger(ALGEBRICKS_LOGGER_NAME);
    public static final String HYRACKS_APP_NAME = "algebricks";

    // public static final Level ALGEBRICKS_LOG_LEVEL = Level.FINEST;
    //
    // static {
    // Handler h;
    // try {
    // h = new ConsoleHandler();
    // h.setFormatter(new SysoutFormatter());
    // } catch (Exception e) {
    // h = new ConsoleHandler();
    // }
    // h.setLevel(ALGEBRICKS_LOG_LEVEL);
    // ALGEBRICKS_LOGGER.addHandler(h);
    // ALGEBRICKS_LOGGER.setLevel(ALGEBRICKS_LOG_LEVEL);
    // }
}
