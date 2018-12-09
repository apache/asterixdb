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
package org.apache.asterix.api.common;

import static org.apache.asterix.api.common.AsterixHyracksIntegrationUtil.LoggerHolder.LOGGER;
import static org.apache.hyracks.util.file.FileUtil.joinPath;

public class SslAsterixHyracksIntegrationUtil extends AsterixHyracksIntegrationUtil {

    public static final String SSL_CONF_FILE = joinPath(RESOURCES_PATH, "cc-ssl.conf");

    public static void main(String[] args) {
        final AsterixHyracksIntegrationUtil integrationUtil = new AsterixHyracksIntegrationUtil();
        try {
            integrationUtil.run(Boolean.getBoolean("cleanup.start"), Boolean.getBoolean("cleanup.shutdown"),
                    System.getProperty("external.lib", ""), System.getProperty("conf.path", SSL_CONF_FILE));
        } catch (Exception e) {
            LOGGER.fatal("Unexpected exception", e);
            System.exit(1);
        }
    }
}