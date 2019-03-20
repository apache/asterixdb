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
package org.apache.hyracks.api.application;

import org.apache.hyracks.api.config.IConfigManager;
import org.apache.logging.log4j.Level;
import org.kohsuke.args4j.OptionHandlerFilter;

@SuppressWarnings("squid:S00112") // define and throw specific class of Exception
public interface IApplication {

    void init(IServiceContext serviceCtx) throws Exception;

    void start(String[] args) throws Exception;

    void startupCompleted() throws Exception;

    void stop() throws Exception;

    void registerConfig(IConfigManager configManager);

    Object getApplicationContext();

    default OptionHandlerFilter getUsageFilter() {
        return OptionHandlerFilter.PUBLIC;
    }

    /**
     * Configures the application loggers with the given level.
     *
     * @param level the logging level desired.
     */
    void configureLoggingLevel(Level level);
}
