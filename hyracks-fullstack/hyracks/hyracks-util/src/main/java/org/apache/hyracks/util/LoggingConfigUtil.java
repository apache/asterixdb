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
package org.apache.hyracks.util;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;

public class LoggingConfigUtil {

    private static final Logger LOGGER = LogManager.getLogger();

    private LoggingConfigUtil() {
    }

    public static void defaultIfMissing(String logger, Level defaultLvl) {
        final Configuration loggingConfig = LoggerContext.getContext(false).getConfiguration();
        final LoggerConfig loggerConfig = loggingConfig.getLoggers().get(logger);
        if (loggerConfig != null) {
            LOGGER.info("{} log level is {}", logger, loggerConfig.getLevel());
        } else {
            LOGGER.info("Setting {} log level to {}", logger, defaultLvl);
            Configurator.setLevel(logger, defaultLvl);
        }
    }
}
