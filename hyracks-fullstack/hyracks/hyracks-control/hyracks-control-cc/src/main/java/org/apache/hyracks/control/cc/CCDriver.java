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
package org.apache.hyracks.control.cc;

import static org.apache.hyracks.control.common.controllers.CCConfig.Option.APP_CLASS;

import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.application.ICCApplicationEntryPoint;
import org.apache.hyracks.control.common.config.ConfigManager;
import org.apache.hyracks.control.common.config.ConfigUtils;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.kohsuke.args4j.CmdLineException;

public class CCDriver {
    private static final Logger LOGGER = Logger.getLogger(CCDriver.class.getName());

    private CCDriver() {
    }

    public static void main(String[] args) throws Exception {
        try {
            final ConfigManager configManager = new ConfigManager(args);
            ICCApplicationEntryPoint appEntryPoint = getAppEntryPoint(args);
            appEntryPoint.registerConfig(configManager);
            CCConfig ccConfig = new CCConfig(configManager);
            ClusterControllerService ccService = new ClusterControllerService(ccConfig, appEntryPoint);
            ccService.start();
            while (true) {
                Thread.sleep(100000);
            }
        } catch (CmdLineException e) {
            LOGGER.log(Level.FINE, "Exception parsing command line: " + Arrays.toString(args), e);
            System.exit(2);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Exiting NCDriver due to exception", e);
            System.exit(1);
        }
    }

    private static ICCApplicationEntryPoint getAppEntryPoint(String[] args)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
        // determine app class so that we can use the correct implementation of the configuration...
        String appClassName = ConfigUtils.getOptionValue(args, APP_CLASS);
        return appClassName != null ? (ICCApplicationEntryPoint) (Class.forName(appClassName)).newInstance()
                : CCApplicationEntryPoint.INSTANCE;
    }
}
