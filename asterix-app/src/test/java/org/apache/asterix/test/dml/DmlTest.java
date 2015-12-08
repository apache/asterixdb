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
package org.apache.asterix.test.dml;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.api.java.AsterixJavaClient;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.compiler.provider.AqlCompilationProvider;
import org.apache.asterix.test.aql.TestExecutor;
import org.apache.asterix.test.base.AsterixTestHelper;
import org.junit.Test;

public class DmlTest {

    private static final String[] ASTERIX_DATA_DIRS = new String[] { "nc1data", "nc2data" };
    private static final String PATH_ACTUAL = "dmltest" + File.separator;
    private static final String SEPARATOR = File.separator;
    private static final String PATH_BASE = "src" + SEPARATOR + "test" + SEPARATOR + "resources" + SEPARATOR + "dmlts"
            + SEPARATOR;
    private static final String PATH_SCRIPTS = PATH_BASE + "scripts" + SEPARATOR;
    private static final String LOAD_FOR_ENLIST_FILE = PATH_SCRIPTS + "load-cust.aql";

    private static final PrintWriter ERR = new PrintWriter(System.err);
    private final TestExecutor testExecutor = new TestExecutor();

    @Test
    public void enlistTest() throws Exception {
        File outdir = new File(PATH_ACTUAL);
        if (outdir.exists()) {
            AsterixTestHelper.deleteRec(outdir);
        }
        outdir.mkdirs();

        AsterixHyracksIntegrationUtil.init(true);
        Reader loadReader = new BufferedReader(
                new InputStreamReader(new FileInputStream(LOAD_FOR_ENLIST_FILE), "UTF-8"));
        AsterixJavaClient asterixLoad = new AsterixJavaClient(
                AsterixHyracksIntegrationUtil.getHyracksClientConnection(), loadReader, ERR,
                new AqlCompilationProvider());
        try {
            asterixLoad.compile(true, false, false, false, false, true, false);
        } catch (AsterixException e) {
            throw new Exception("Compile ERROR for " + LOAD_FOR_ENLIST_FILE + ": " + e.getMessage(), e);
        } finally {
            loadReader.close();
        }
        asterixLoad.execute();

        AsterixHyracksIntegrationUtil.deinit(true);
        for (String d : ASTERIX_DATA_DIRS) {
            testExecutor.deleteRec(new File(d));
        }
        outdir.delete();
    }
}
