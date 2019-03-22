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
package org.apache.asterix.external.parser.test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.provider.StreamRecordReaderProvider;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.junit.Assert;
import org.junit.Test;

public class StreamRecordReaderProviderTest {

    @Test
    public void Test() throws AsterixException {
        List<String> recordReaderFormats =
                Arrays.asList(ExternalDataConstants.FORMAT_LINE_SEPARATED, ExternalDataConstants.FORMAT_ADM,
                        ExternalDataConstants.FORMAT_JSON_LOWER_CASE, ExternalDataConstants.FORMAT_SEMISTRUCTURED,
                        ExternalDataConstants.FORMAT_DELIMITED_TEXT, ExternalDataConstants.FORMAT_CSV);
        Map<String, String> config = new HashMap<>();
        for (String format : recordReaderFormats) {
            config.clear();
            config.put(ExternalDataConstants.KEY_FORMAT, format);
            Class clazz = StreamRecordReaderProvider.getRecordReaderClazz(config);
            Assert.assertTrue(clazz != null);
        }
        config.clear();
        config.put(ExternalDataConstants.KEY_FORMAT, ExternalDataConstants.FORMAT_CSV);
        config.put(ExternalDataConstants.KEY_QUOTE, "\u0000");
        Assert.assertTrue(StreamRecordReaderProvider.getRecordReaderClazz(config) != null);
    }
}
