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
package org.apache.hyracks.control.nc;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hyracks.api.application.INCApplication;
import org.apache.hyracks.control.common.config.ConfigManager;
import org.apache.hyracks.control.common.config.ConfigUtils;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.kohsuke.args4j.CmdLineException;

@SuppressWarnings("InfiniteLoopStatement")
public class NCDriver {
    private static final Logger LOGGER = LogManager.getLogger();

    private NCDriver() {
    }

    public static void main(String[] args) {
        try {
            final String nodeId = ConfigUtils.getOptionValue(args, NCConfig.Option.NODE_ID);
            final ConfigManager configManager = new ConfigManager(args);
            INCApplication application = getApplication(args);
            application.registerConfig(configManager);
            NCConfig ncConfig = new NCConfig(nodeId, configManager);
            LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
            Configuration cfg = ctx.getConfiguration();
            NCLogConfigurationFactory logCfgFactory = new NCLogConfigurationFactory(ncConfig);
            ConfigurationFactory.setConfigurationFactory(logCfgFactory);
            configManager.processConfig();
            cfg.removeLogger("Console");
            ctx.start(logCfgFactory.getConfiguration(ctx, ConfigurationSource.NULL_SOURCE));
            final NodeControllerService ncService = new NodeControllerService(ncConfig, application);
            ncService.start();
            while (true) {
                Thread.sleep(10000);
            }
        } catch (CmdLineException e) {
            LOGGER.log(Level.DEBUG, "Exception parsing command line: " + Arrays.toString(args), e);
            System.exit(2);
        } catch (Exception e) {
            LOGGER.error("Exiting NCDriver due to exception", e);
            System.exit(1);
        }
    }

    private static INCApplication getApplication(String[] args)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
        // determine app class so that we can use the correct implementation of the configuration...
        String appClassName = ConfigUtils.getOptionValue(args, NCConfig.Option.APP_CLASS);
        return appClassName != null ? (INCApplication) (Class.forName(appClassName)).newInstance()
                : BaseNCApplication.INSTANCE;
    }
}
