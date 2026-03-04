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
package org.apache.asterix.api.http.server;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.common.config.ExternalProperties;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class MissingCredentialFileTest {

    private static final String MISSING_CREDENTIAL_FILE = "target/no-such-dir/passwd";

    private final AsterixHyracksIntegrationUtil integrationUtil = new AsterixHyracksIntegrationUtil();

    @After
    public void tearDown() throws Exception {
        try {
            integrationUtil.deinit(true);
        } finally {
            integrationUtil.clearOptions();
        }
    }

    @Test
    public void startupFailsWhenAuthenticationIsEnabled() {
        integrationUtil.addOption(ExternalProperties.Option.API_AUTHENTICATION_ENABLED, true);
        integrationUtil.addOption(CCConfig.Option.CREDENTIAL_FILE, MISSING_CREDENTIAL_FILE);
        integrationUtil.addOption(NCConfig.Option.CREDENTIAL_FILE, MISSING_CREDENTIAL_FILE);
        Exception e = Assert.assertThrows(Exception.class,
                () -> integrationUtil.init(true, AsterixHyracksIntegrationUtil.DEFAULT_CONF_FILE));
        Assert.assertTrue(String.valueOf(e), hasMessage(e, "no-such-dir"));
    }

    private static boolean hasMessage(Throwable th, String fragment) {
        for (Throwable t = th; t != null; t = t.getCause()) {
            if (t.getMessage() != null && t.getMessage().contains(fragment)) {
                return true;
            }
        }
        return false;
    }
}
