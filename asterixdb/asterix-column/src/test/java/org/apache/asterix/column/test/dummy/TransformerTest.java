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
package org.apache.asterix.column.test.dummy;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;

import org.apache.asterix.column.common.test.TestCase;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.visitor.SchemaStringBuilderVisitor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TransformerTest extends AbstractDummyTest {

    public TransformerTest(TestCase testCase) throws HyracksDataException {
        super(testCase);
    }

    /*
     * ***********************************************************************
     * Setup
     * ***********************************************************************
     */

    @BeforeClass
    public static void setup() throws IOException {
        setup(TransformerTest.class);
    }

    @Parameters(name = "TransformerTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        return initTests(TransformerTest.class, "transformer");
    }

    /*
     * ***********************************************************************
     * Test
     * ***********************************************************************
     */

    @Test
    public void runTest() throws IOException {
        File testFile = testCase.getTestFile();
        prepareParser(testFile);
        ObjectSchemaNode node = transform();
        writeResult(node);
        testCase.compare();
    }

    private void writeResult(ObjectSchemaNode root) throws IOException {
        File resultFile = testCase.getOutputFile();
        SchemaStringBuilderVisitor schemaBuilder = new SchemaStringBuilderVisitor(columnMetadata);
        String schema = schemaBuilder.build(root);

        try (PrintStream ps = new PrintStream(new FileOutputStream(resultFile))) {
            ps.print(schema);
        }
    }
}
