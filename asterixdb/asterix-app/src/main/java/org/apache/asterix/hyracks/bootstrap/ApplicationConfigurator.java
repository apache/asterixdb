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
package org.apache.asterix.hyracks.bootstrap;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Properties;

import org.apache.asterix.common.config.AsterixProperties;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.hyracks.api.config.IConfigManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.ControllerConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.common.utils.ConfigurationUtil;
import org.apache.hyracks.util.file.FileUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class ApplicationConfigurator {
    private static final Logger LOGGER = LogManager.getLogger();

    private ApplicationConfigurator() {
    }

    static void registerConfigOptions(IConfigManager configManager) {
        AsterixProperties.registerConfigOptions(configManager);
        ControllerConfig.Option.DEFAULT_DIR
                .setDefaultValue(FileUtil.joinPath(System.getProperty(ConfigurationUtil.JAVA_IO_TMPDIR), "asterixdb"));
        NCConfig.Option.APP_CLASS.setDefaultValue(NCApplication.class.getName());
        CCConfig.Option.APP_CLASS.setDefaultValue(CCApplication.class.getName());
        try {
            InputStream propertyStream =
                    ApplicationConfigurator.class.getClassLoader().getResourceAsStream("git.properties");
            if (propertyStream != null) {
                Properties gitProperties = new Properties();
                gitProperties.load(propertyStream);
                StringWriter sw = new StringWriter();
                gitProperties.store(sw, null);
                configManager.setVersionString(sw.toString());
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

    }

    public static void validateJavaRuntime() throws HyracksDataException {
        final String javaVersion = System.getProperty("java.version");
        LOGGER.info("Found JRE version " + javaVersion);
        String[] splits = javaVersion.split("\\.");
        if ("1".equals(splits[0])) {
            switch (splits[1]) {
                case "9":
                    LOGGER.warn("JRE version \"" + javaVersion + "\" is untested");
                    //fall-through
                case "8":
                    return;
                default:
                    throw RuntimeDataException.create(ErrorCode.UNSUPPORTED_JRE,
                            "a minimum version of JRE of 1.8 is required, but is currently: \"" + javaVersion + "\"");
            }
        } else {
            LOGGER.warn("JRE version \"" + javaVersion + "\" is untested");
        }
    }
}
