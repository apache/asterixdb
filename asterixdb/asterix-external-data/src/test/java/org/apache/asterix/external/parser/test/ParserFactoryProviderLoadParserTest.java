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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.IDataParserFactory;
import org.apache.asterix.external.parser.factory.ADMDataParserFactory;
import org.apache.asterix.external.parser.factory.DelimitedDataParserFactory;
import org.apache.asterix.external.parser.factory.HiveDataParserFactory;
import org.apache.asterix.external.parser.factory.RSSParserFactory;
import org.apache.asterix.external.parser.factory.TweetParserFactory;
import org.apache.asterix.external.provider.ParserFactoryProvider;
import org.junit.Assert;
import org.junit.Test;

public class ParserFactoryProviderLoadParserTest {

    IDataParserFactory factory;

    @Test
    public void test() throws AsterixException {
        boolean result = true;
        factory = ParserFactoryProvider.getDataParserFactory("csv");
        result = result && factory instanceof DelimitedDataParserFactory;
        factory = ParserFactoryProvider.getDataParserFactory("adm");
        result = result && factory instanceof ADMDataParserFactory;
        factory = ParserFactoryProvider.getDataParserFactory("rss");
        result = result && factory instanceof RSSParserFactory;
        factory = ParserFactoryProvider.getDataParserFactory("hive");
        result = result && factory instanceof HiveDataParserFactory;
        factory = ParserFactoryProvider.getDataParserFactory("twitter-status");
        result = result && factory instanceof TweetParserFactory;
        Assert.assertTrue(result);
    }
}
