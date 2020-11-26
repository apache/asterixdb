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

package org.apache.asterix.test.sqlpp;

import java.util.List;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.DataverseNameTest;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.sqlpp.parser.SqlppParserFactory;
import org.junit.Assert;

public class DataverseNameParserTest extends DataverseNameTest {

    private final IParserFactory parserFactory = new SqlppParserFactory();

    @Override
    protected void testDataverseNameImpl(DataverseName dataverseName, List<String> parts, String expectedCanonicalForm,
            String expectedDisplayForm) throws Exception {
        super.testDataverseNameImpl(dataverseName, parts, expectedCanonicalForm, expectedDisplayForm);

        String displayForm = dataverseName.toString();

        // check parse-ability of the display form
        IParser parser = parserFactory.createParser(displayForm);
        List<String> parsedParts = parser.parseMultipartIdentifier();

        int partCount = parts.size();
        Assert.assertEquals("Unexpected parsed part count: " + parsedParts.size(), partCount, parsedParts.size());

        for (int i = 0; i < partCount; i++) {
            String part = parts.get(i);
            String parsedPart = parsedParts.get(i);
            Assert.assertEquals("unexpected parsed part at position " + i + " in " + parts, part, parsedPart);
        }
    }
}
