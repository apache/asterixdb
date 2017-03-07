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
package org.apache.hyracks.api.config;

import java.util.Set;
import java.util.function.Predicate;

import org.kohsuke.args4j.OptionHandlerFilter;

public interface IConfigManager {
    int PARSE_INI_POINTERS_METRIC = 100;
    int PARSE_INI_METRIC = 200;
    int PARSE_COMMAND_LINE_METRIC = 300;
    int APPLY_DEFAULTS_METRIC = 400;

    void register(IOption... options);

    @SuppressWarnings("unchecked")
    void register(Class<? extends IOption>... optionClasses);

    Set<Section> getSections(Predicate<Section> predicate);

    Set<Section> getSections();

    Set<IOption> getOptions(Section section);

    IApplicationConfig getAppConfig();

    void addConfigurator(int metric, IConfigurator configurator);

    void addIniParamOptions(IOption... options);

    void addCmdLineSections(Section... sections);

    void setUsageFilter(OptionHandlerFilter usageFilter);
}
