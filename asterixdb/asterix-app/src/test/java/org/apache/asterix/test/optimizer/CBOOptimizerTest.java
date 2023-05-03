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
package org.apache.asterix.test.optimizer;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Runs the optimization tests with CBO enabled. It compares a test's actual result against the expected result in
 * {@link CBOOptimizerTest#CBO_PATH_EXPECTED} if one is provided. Otherwise, it compares against the expected result in
 * {@link OptimizerTest#PATH_EXPECTED}.
 */
@RunWith(Parameterized.class)
public class CBOOptimizerTest extends OptimizerTest {

    private static final String CBO_PATH_EXPECTED = PATH_BASE + "results_cbo" + SEPARATOR;
    static {
        TEST_CONFIG_FILE_NAME = "src/test/resources/cc-cbotest.conf";
        EXTENSION_RESULT = "plan";
        PATH_ACTUAL = "target" + SEPARATOR + "cbo_opttest" + SEPARATOR;
    }

    @Parameters(name = "CBOOptimizerTest {index}: {0}")
    public static Collection<Object[]> tests() {
        return AbstractOptimizerTest.tests();
    }

    public CBOOptimizerTest(File queryFile, String expectedFilePath, File actualFile) {
        super(queryFile, expectedFilePath, actualFile);
    }

    @Override
    protected List<String> getExpectedLines() throws IOException {
        Path cboFilePath = Path.of(CBO_PATH_EXPECTED, expectedFilePath);
        return Files.exists(cboFilePath) ? Files.readAllLines(cboFilePath, StandardCharsets.UTF_8)
                : super.getExpectedLines();
    }
}
