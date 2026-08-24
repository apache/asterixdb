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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.Collections;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.hyracks.algebricks.core.config.AlgebricksConfig;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.control.common.application.ConfigManagerApplicationConfig;
import org.apache.hyracks.control.common.config.ConfigManager;
import org.junit.Test;

public class OptimizationConfUtilTest {

    private static final int ABOVE_MAX_JOINS = AlgebricksConfig.CBO_MAX_JOINS_UPPER_BOUND + 1;

    @Test
    public void maxJoinsFallsBackToTheDefault() throws Exception {
        assertEquals(AlgebricksConfig.CBO_MAX_JOINS_DEFAULT, createConf(null, Collections.emptyMap()).getCBOMaxJoins());
    }

    @Test
    public void maxJoinsIsTakenFromTheClusterConfiguration() throws Exception {
        assertEquals(7, createConf(7, Collections.emptyMap()).getCBOMaxJoins());
    }

    @Test
    public void statementSettingOverridesTheClusterConfiguration() throws Exception {
        assertEquals(5, createConf(7, Map.of(CompilerProperties.COMPILER_CBO_MAXJOINS_KEY, "5")).getCBOMaxJoins());
    }

    @Test
    public void statementSettingAboveTheUpperBoundIsRejected() {
        assertThrows(AsterixException.class, () -> createConf(null,
                Map.of(CompilerProperties.COMPILER_CBO_MAXJOINS_KEY, String.valueOf(ABOVE_MAX_JOINS))));
    }

    @Test
    public void statementSettingBelowASingleJoinIsRejected() {
        assertThrows(AsterixException.class,
                () -> createConf(null, Map.of(CompilerProperties.COMPILER_CBO_MAXJOINS_KEY, "0")));
    }

    @Test
    public void clusterConfigurationCarriesTheSameBound() {
        IOptionType<Integer> type = CompilerProperties.Option.COMPILER_CBO_MAXJOINS.type();
        assertThrows(IllegalArgumentException.class, () -> type.parse(String.valueOf(ABOVE_MAX_JOINS)));
        assertThrows(IllegalArgumentException.class, () -> type.parse("0"));
    }

    /**
     * @param clusterMaxJoins the value configured on the cluster, or {@code null} to leave it unset
     * @param statementConfig the settings the statement carries
     */
    private static PhysicalOptimizationConfig createConf(Integer clusterMaxJoins, Map<String, Object> statementConfig)
            throws Exception {
        ConfigManager configManager = new ConfigManager();
        configManager.register(CompilerProperties.Option.values());
        if (clusterMaxJoins != null) {
            configManager.set(CompilerProperties.Option.COMPILER_CBO_MAXJOINS, clusterMaxJoins);
        }
        IApplicationConfig applicationConfig = new ConfigManagerApplicationConfig(configManager);
        CompilerProperties compilerProperties =
                new CompilerProperties(PropertiesAccessor.getInstance(applicationConfig));
        return OptimizationConfUtil.createPhysicalOptimizationConf(compilerProperties, statementConfig,
                Collections.emptySet(), null);
    }
}
