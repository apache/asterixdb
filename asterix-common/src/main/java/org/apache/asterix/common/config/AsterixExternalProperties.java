/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.common.config;

import java.util.logging.Level;

public class AsterixExternalProperties extends AbstractAsterixProperties {

    private static final String EXTERNAL_WEBPORT_KEY = "web.port";
    private static int EXTERNAL_WEBPORT_DEFAULT = 19001;

    private static final String EXTERNAL_LOGLEVEL_KEY = "log.level";
    private static Level EXTERNAL_LOGLEVEL_DEFAULT = Level.WARNING;

    private static final String EXTERNAL_APISERVER_KEY = "api.port";
    private static int EXTERNAL_APISERVER_DEFAULT = 19002;

    private static final String EXTERNAL_FEEDSERVER_KEY = "feed.port";
    private static int EXTERNAL_FEEDSERVER_DEFAULT = 19003;

    private static final String EXTERNAL_CC_JAVA_OPTS_KEY = "cc.java.opts";
    private static String EXTERNAL_CC_JAVA_OPTS_DEFAULT = "-Xmx1024m";

    private static final String EXTERNAL_NC_JAVA_OPTS_KEY = "nc.java.opts";
    private static String EXTERNAL_NC_JAVA_OPTS_DEFAULT = "-Xmx1024m";

    private static final String EXTERNAL_MAX_WAIT_FOR_ACTIVE_CLUSTER = "max.wait.active.cluster";
    private static int EXTERNAL_MAX_WAIT_FOR_ACTIVE_CLUSTER_DEFAULT = 60;

    private static final String EXTERNAL_PLOT_ACTIVATE = "plot.activate";
    private static Boolean EXTERNAL_PLOT_ACTIVATE_DEFAULT = new Boolean(false);

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

    public int getFeedServerPort() {
        return accessor.getProperty(EXTERNAL_FEEDSERVER_KEY, EXTERNAL_FEEDSERVER_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public Level getLogLevel() {
        return accessor.getProperty(EXTERNAL_LOGLEVEL_KEY, EXTERNAL_LOGLEVEL_DEFAULT,
                PropertyInterpreters.getLevelPropertyInterpreter());
    }

    public String getNCJavaParams() {
        return accessor.getProperty(EXTERNAL_NC_JAVA_OPTS_KEY, EXTERNAL_NC_JAVA_OPTS_DEFAULT,
                PropertyInterpreters.getStringPropertyInterpreter());
    }

    public String getCCJavaParams() {
        return accessor.getProperty(EXTERNAL_CC_JAVA_OPTS_KEY, EXTERNAL_CC_JAVA_OPTS_DEFAULT,
                PropertyInterpreters.getStringPropertyInterpreter());
    }

    public int getMaxWaitClusterActive() {
        return accessor.getProperty(EXTERNAL_MAX_WAIT_FOR_ACTIVE_CLUSTER, EXTERNAL_MAX_WAIT_FOR_ACTIVE_CLUSTER_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public Boolean getIsPlottingEnabled() {
        return accessor.getProperty(EXTERNAL_PLOT_ACTIVATE, EXTERNAL_PLOT_ACTIVATE_DEFAULT,
                PropertyInterpreters.getBooleanPropertyInterpreter());
    }

}
