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

import java.util.logging.Level;

public class AsterixExternalProperties extends AbstractAsterixProperties {

    private static final String EXTERNAL_WEBPORT_KEY = "web.port";
    private static int EXTERNAL_WEBPORT_DEFAULT = 19001;

    private static final String EXTERNAL_LOGLEVEL_KEY = "log.level";
    private static Level EXTERNAL_LOGLEVEL_DEFAULT = Level.WARNING;

    private static final String EXTERNAL_APISERVER_KEY = "api.port";
    private static int EXTERNAL_APISERVER_DEFAULT = 19002;

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
