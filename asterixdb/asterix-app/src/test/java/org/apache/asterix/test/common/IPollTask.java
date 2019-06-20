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
package org.apache.asterix.test.common;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.context.TestFileContext;
import org.apache.asterix.testframework.xml.TestCase.CompilationUnit;
import org.apache.commons.lang3.mutable.MutableInt;

public interface IPollTask {

    /**
     * Execute the poll task
     *
     * @param testCaseCtx
     * @param ctx
     * @param variableCtx
     * @param statement
     * @param isDmlRecoveryTest
     * @param pb
     * @param cUnit
     * @param queryCount
     * @param expectedResultFileCtxs
     * @param testFile
     * @param actualPath
     * @param actualWarnCount
     */
    void execute(TestCaseContext testCaseCtx, TestFileContext ctx, Map<String, Object> variableCtx, String statement,
            boolean isDmlRecoveryTest, ProcessBuilder pb, CompilationUnit cUnit, MutableInt queryCount,
            List<TestFileContext> expectedResultFileCtxs, File testFile, String actualPath, MutableInt actualWarnCount)
            throws Exception;

}
