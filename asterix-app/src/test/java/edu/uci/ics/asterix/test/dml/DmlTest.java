/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.test.dml;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;

import edu.uci.ics.asterix.api.common.AsterixHyracksIntegrationUtil;
import edu.uci.ics.asterix.api.java.AsterixJavaClient;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.test.aql.TestsUtils;
import edu.uci.ics.asterix.test.base.AsterixTestHelper;

public class DmlTest {

    private static final String[] ASTERIX_DATA_DIRS = new String[] { "nc1data", "nc2data" };
    private static final String PATH_ACTUAL = "dmltest/";
    private static final String SEPARATOR = File.separator;
    private static final String PATH_BASE = "src" + SEPARATOR + "test" + SEPARATOR + "resources" + SEPARATOR + "dmlts"
            + SEPARATOR;
    private static final String PATH_EXPECTED = PATH_BASE + "results" + SEPARATOR;
    private static final String PATH_SCRIPTS = PATH_BASE + "scripts" + SEPARATOR;
    private static final String LOAD_FOR_ENLIST_FILE = PATH_SCRIPTS + "load-cust.aql";
    private static final String ENLIST_FILE = PATH_SCRIPTS + "enlist-scan-cust.aql";

    private static final PrintWriter ERR = new PrintWriter(System.err);

    public void enlistTest() throws Exception {
        File outdir = new File(PATH_ACTUAL);
        if (outdir.exists()) {
            AsterixTestHelper.deleteRec(outdir);
        }
        outdir.mkdirs();

        AsterixHyracksIntegrationUtil.init();
        Reader loadReader = new BufferedReader(
                new InputStreamReader(new FileInputStream(LOAD_FOR_ENLIST_FILE), "UTF-8"));
        AsterixJavaClient asterixLoad = new AsterixJavaClient(
                AsterixHyracksIntegrationUtil.getHyracksClientConnection(), loadReader, ERR);
        try {
            asterixLoad.compile(true, false, false, false, false, true, false);
        } catch (AsterixException e) {
            throw new Exception("Compile ERROR for " + LOAD_FOR_ENLIST_FILE + ": " + e.getMessage(), e);
        } finally {
            loadReader.close();
        }
        asterixLoad.execute();
        File enlistFile = new File(ENLIST_FILE);
        String resultFileName = TestsUtils.aqlExtToResExt(enlistFile.getName());
        File expectedFile = new File(PATH_EXPECTED + SEPARATOR + resultFileName);
        File actualFile = new File(PATH_ACTUAL + SEPARATOR + resultFileName);
        // Khurram
        //TestsUtils.runScriptAndCompareWithResult(AsterixHyracksIntegrationUtil.getHyracksClientConnection(),
                //enlistFile, ERR, expectedFile, actualFile);

        AsterixHyracksIntegrationUtil.deinit();
        for (String d : ASTERIX_DATA_DIRS) {
            TestsUtils.deleteRec(new File(d));
        }
        outdir.delete();
    }
}
