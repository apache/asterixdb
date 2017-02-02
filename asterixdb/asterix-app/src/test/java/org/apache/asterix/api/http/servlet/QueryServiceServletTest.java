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

package org.apache.asterix.api.http.servlet;

import org.apache.asterix.api.http.server.QueryServiceServlet;
import org.junit.Assert;
import org.junit.Test;

public class QueryServiceServletTest {

    @Test
    public void testTimeUnitFormatNanos() throws Exception {
        Assert.assertEquals("123.456789012s", QueryServiceServlet.TimeUnit.formatNanos(123456789012l));
        Assert.assertEquals("12.345678901s", QueryServiceServlet.TimeUnit.formatNanos(12345678901l));
        Assert.assertEquals("1.234567890s", QueryServiceServlet.TimeUnit.formatNanos(1234567890l));
        Assert.assertEquals("123.456789ms", QueryServiceServlet.TimeUnit.formatNanos(123456789l));
        Assert.assertEquals("12.345678ms", QueryServiceServlet.TimeUnit.formatNanos(12345678l));
        Assert.assertEquals("1.234567ms", QueryServiceServlet.TimeUnit.formatNanos(1234567l));
        Assert.assertEquals("123.456µs", QueryServiceServlet.TimeUnit.formatNanos(123456l));
        Assert.assertEquals("12.345µs", QueryServiceServlet.TimeUnit.formatNanos(12345l));
        Assert.assertEquals("1.234µs", QueryServiceServlet.TimeUnit.formatNanos(1234l));
        Assert.assertEquals("123ns", QueryServiceServlet.TimeUnit.formatNanos(123l));
        Assert.assertEquals("12ns", QueryServiceServlet.TimeUnit.formatNanos(12l));
        Assert.assertEquals("1ns", QueryServiceServlet.TimeUnit.formatNanos(1l));
        Assert.assertEquals("-123.456789012s", QueryServiceServlet.TimeUnit.formatNanos(-123456789012l));
        Assert.assertEquals("120.000000000s", QueryServiceServlet.TimeUnit.formatNanos(120000000000l));
        Assert.assertEquals("-12ns", QueryServiceServlet.TimeUnit.formatNanos(-12l));
    }
}
