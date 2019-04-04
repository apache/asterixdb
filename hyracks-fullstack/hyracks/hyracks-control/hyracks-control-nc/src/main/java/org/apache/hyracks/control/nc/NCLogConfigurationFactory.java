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

import java.io.File;
import java.net.URI;

import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.LayoutComponentBuilder;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;

public class NCLogConfigurationFactory extends ConfigurationFactory {
    private static final Logger LOGGER = LogManager.getLogger();
    private NCConfig config;

    public NCLogConfigurationFactory(NCConfig config) {
        this.config = config;
    }

    public Configuration createConfiguration(ConfigurationBuilder<BuiltConfiguration> builder) {
        String nodeId = config.getNodeId();
        File logDir = new File(config.getLogDir());
        File ncLog = new File(logDir, "nc-" + nodeId + ".log");
        LOGGER.warn("logs are being redirected to: {}", ncLog::getAbsolutePath);
        builder.setStatusLevel(Level.WARN);
        builder.setConfigurationName("RollingBuilder");
        // create a rolling file appender
        LayoutComponentBuilder defaultLayout = builder.newLayout("PatternLayout").addAttribute("pattern",
                "%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n");
        ComponentBuilder triggeringPolicy = builder.newComponent("Policies")
                .addComponent(builder.newComponent("CronTriggeringPolicy").addAttribute("schedule", "0 0 0 * * ?"))
                .addComponent(builder.newComponent("SizeBasedTriggeringPolicy").addAttribute("size", "50M"));
        AppenderComponentBuilder defaultRoll =
                builder.newAppender("default", "RollingFile").addAttribute("fileName", ncLog.getAbsolutePath())
                        .addAttribute("filePattern",
                                new File(logDir, "nc-" + nodeId + "-%d{MM-dd-yy-ss}.log.gz").getAbsolutePath())
                        .add(defaultLayout).addComponent(triggeringPolicy);
        builder.add(defaultRoll);

        // create the new logger
        builder.add(builder.newRootLogger(Level.INFO).add(builder.newAppenderRef("default")));

        LayoutComponentBuilder accessLayout = builder.newLayout("PatternLayout").addAttribute("pattern", "%m%n");
        AppenderComponentBuilder accessRoll = builder.newAppender("access", "RollingFile")
                .addAttribute("fileName", new File(logDir, "access-" + nodeId + ".log").getAbsolutePath())
                .addAttribute("filePattern",
                        new File(logDir, "access-" + nodeId + "-%d{MM-dd-yy-ss}.log.gz").getAbsolutePath())
                .add(accessLayout).addComponent(triggeringPolicy);
        builder.add(accessRoll);
        builder.add(builder.newLogger("org.apache.hyracks.http.server.CLFLogger", Level.forName("ACCESS", 550))
                .add(builder.newAppenderRef("access")).addAttribute("additivity", false));

        LayoutComponentBuilder traceLayout = builder.newLayout("PatternLayout").addAttribute("pattern", "%m,%n")
                .addAttribute("header", "[").addAttribute("footer", "]");
        AppenderComponentBuilder traceRoll = builder.newAppender("trace", "RollingFile")
                .addAttribute("fileName", new File(logDir, "trace-" + nodeId + ".log"))
                .addAttribute("filePattern", new File(logDir, "trace-" + nodeId + "-%d{MM-dd-yy-ss}.log.gz"))
                .add(traceLayout).addComponent(triggeringPolicy);
        builder.add(traceRoll);
        builder.add(builder.newLogger("org.apache.hyracks.util.trace.Tracer.Traces", Level.forName("TRACER", 570))
                .add(builder.newAppenderRef("trace")).addAttribute("additivity", false));

        return builder.build();
    }

    @Override
    public Configuration getConfiguration(final LoggerContext loggerContext, final ConfigurationSource source) {
        return getConfiguration(loggerContext, source.toString(), null);
    }

    @Override
    public Configuration getConfiguration(final LoggerContext loggerContext, final String name,
            final URI configLocation) {
        ConfigurationBuilder<BuiltConfiguration> builder = newConfigurationBuilder();
        return createConfiguration(builder);
    }

    @Override
    protected String[] getSupportedTypes() {
        return new String[] { "*" };
    }

    @Override
    public String toString() {
        return "NCLogConfiguration";
    }
}
