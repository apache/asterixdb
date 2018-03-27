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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.hyracks.api.config.IConfigManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class AsterixProperties {

    public static final String SECTION_PREFIX_EXTENSION = "extension/";

    private AsterixProperties() {
    }

    public static String getSectionId(String prefix, String section) {
        return section.substring(prefix.length());
    }

    public static void registerConfigOptions(IConfigManager configManager) {
        configManager.register(NodeProperties.Option.class, CompilerProperties.Option.class,
                MetadataProperties.Option.class, ExternalProperties.Option.class, ActiveProperties.Option.class,
                MessagingProperties.Option.class, ReplicationProperties.Option.class, StorageProperties.Option.class,
                TransactionProperties.Option.class);

        // we need to process the old-style asterix config before we apply defaults!
        configManager.addConfigurator(IConfigManager.ConfiguratorMetric.APPLY_DEFAULTS.metric() - 1, () -> {
            try {
                PropertiesAccessor.getInstance(configManager.getAppConfig());
            } catch (AsterixException e) {
                throw HyracksDataException.create(e);
            }
        });
    }
}
