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
package org.apache.hyracks.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class NetworkUtilTest {

    private static final Logger LOGGER = LogManager.getLogger();

    @Test
    public void testDefaultPort() {
        Assert.assertEquals("127.0.0.1:1234", NetworkUtil.defaultPort("127.0.0.1:1234", 9999));
        Assert.assertEquals("127.0.0.1:9999", NetworkUtil.defaultPort("127.0.0.1", 9999));
        Assert.assertEquals("[::1]:1234", NetworkUtil.defaultPort("[::1]:1234", 9999));
        Assert.assertEquals("[::1]:9999", NetworkUtil.defaultPort("::1", 9999));
        Assert.assertEquals("localhost.localdomain.local:9999",
                NetworkUtil.defaultPort("localhost.localdomain.local", 9999));
        Assert.assertEquals("localhost.localdomain.local:1234",
                NetworkUtil.defaultPort("localhost.localdomain.local:1234", 9999));

    }
}
