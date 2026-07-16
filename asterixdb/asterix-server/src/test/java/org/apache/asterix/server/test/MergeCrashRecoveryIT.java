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
package org.apache.asterix.server.test;

import static org.apache.hyracks.util.file.FileUtil.joinPath;
import java.io.*;
import java.util.*;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.utils.Servlets;
import org.apache.asterix.test.base.TestMethodTracer;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.test.common.TestHelper;
import org.apache.asterix.testframework.context.TestCaseContext.OutputFormat;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runners.MethodSorters;

public class MergeCrashRecoveryIT {

    // Important paths and files for this test.

    // The "target" subdirectory of asterix-server. All outputs go here.
    private static final String TARGET_DIR = joinPath("target");

    private static final String OUTPUT_DIR = joinPath(TARGET_DIR, "sampleCluster");

    private static final String LOCAL_SAMPLES_DIR = joinPath(OUTPUT_DIR, "opt", "local");

    private String CONFIG_FILE = joinPath(LOCAL_SAMPLES_DIR,"conf/cc.conf");

    private String LINEITEM_PATH = joinPath(System.getProperty("user.dir"),"src","test","resources","MergeCrashRecoveryIT","lineitem.tbl");

    private String DDL_STATEMENT = "drop dataverse test if exists; " +
                                    "create dataverse test; " +
                                    "use dataverse test; " +
                                    "create type LineItemType as closed { l_orderkey: int64, l_partkey: int64, l_suppkey: int64, " +
                                    "l_linenumber: int64, l_quantity: double, l_extendedprice: double, l_discount: double, " +
                                    "l_tax: double, l_returnflag: string, l_linestatus: string, l_shipdate: string, l_commitdate: string, " +
                                    "l_receiptdate: string, l_shipinstruct: string, l_shipmode: string, l_comment: string } " +
                                    "create dataset LineItem(LineItemType) primary key l_orderkey, l_linenumber; " +
                                    "create feed TableFeed " +
                                    "using localfs " +
                                    "(('type-name'='LineItemType'), " +
                                    "('path'='127.0.0.1://"+LINEITEM_PATH+"'), " +
                                    "('format'='delimited-text'),('delimiter'='|')); " +
                                    "connect feed TableFeed to dataset LineItem; " +
                                    "start feed TableFeed;";

    @BeforeClass
    public static void setUp() throws Exception {
        // Create actual-results output directory.
        File outDir = new File(OUTPUT_DIR);
        // Remove any instance data from previous runs.
        if (outDir.isDirectory()) {
            FileUtils.deleteDirectory(outDir);
        }
        outDir.mkdirs();

        String[] pathElements = new String[] { TARGET_DIR,
                new File(TARGET_DIR).list((dir, name) -> name.matches("asterix-server.*-binary-assembly.zip"))[0] };
        String installerZip = joinPath(pathElements);
        TestHelper.unzip(installerZip, OUTPUT_DIR);
    }

    @AfterClass
    public static void cleanUp() throws Exception {
        Process process = new ProcessBuilder(joinPath(LOCAL_SAMPLES_DIR, "bin/stop-sample-cluster.sh"), "-f").inheritIO().start();
        Assert.assertEquals(0, process.waitFor());
    }

    @Test
    public void test() throws Exception {
        // Stop Cluster, if cluster is running
        Process process = new ProcessBuilder(joinPath(LOCAL_SAMPLES_DIR, "bin/stop-sample-cluster.sh"), "-f").inheritIO().start();
        Assert.assertEquals(0, process.waitFor());

        // Edit Config File
        String[] sedCmd1 = {"sed", "-i", "/command=asterixnc/a storage.memorycomponent.numpages=2", CONFIG_FILE};
        String[] sedCmd2 = {"sed", "-i", "/command=asterixnc/a storage.memorycomponent.pagesize=2KB", CONFIG_FILE};
        process = new ProcessBuilder(sedCmd1).inheritIO().start();
        Assert.assertEquals(0, process.waitFor());
        process = new ProcessBuilder(sedCmd2).inheritIO().start();
        Assert.assertEquals(0, process.waitFor());

        // Start Cluster
        process = new ProcessBuilder(joinPath(LOCAL_SAMPLES_DIR, "bin/start-sample-cluster.sh")).inheritIO().start();
        Assert.assertEquals(0, process.waitFor());

        // Create & Load Dataset
        System.out.println("\nCreating dataset ...");
        TestExecutor testExecutor = new TestExecutor();
        testExecutor.executeDDL(DDL_STATEMENT,new URI("http", null, "127.0.0.1", 19002, Servlets.AQL, null, null));

        // Wait for 20 secs so that all the records are inserted.
        Thread.sleep(20000);

        String num_records_before_compact = executeQueryAndGetOutput("use dataverse test; count(for $l in dataset LineItem return $l);");

        // Check if there is more than 1 '_b' file
        String DATA_BLUE_PATH = joinPath(LOCAL_SAMPLES_DIR, "data/blue/storage/partition_0/test/LineItem_idx_LineItem");
        String DATA_RED_PATH = joinPath(LOCAL_SAMPLES_DIR, "data/red/storage/partition_1/test/LineItem_idx_LineItem");

        int part0_files_before = countFilesEndingWith(new File(DATA_BLUE_PATH),"_b");
        int part1_files_before = countFilesEndingWith(new File(DATA_RED_PATH),"_b");

/*        Assert.assertNotEquals(1,part0_files_before);
        Assert.assertNotEquals(1,part1_files_before);*/
        System.out.println("\nNumber of files in partition 0 before COMPACT statement: " + part0_files_before);
        System.out.println("\nNumber of files in partition 1 before COMPACT statement: " + part1_files_before);

        // Issue the COMPACT statement
        String COMPACT_STATEMENT = "use dataverse test; compact dataset LineItem;";
        testExecutor.executeUpdate(COMPACT_STATEMENT, new URI("http", null, "127.0.0.1", 19002, Servlets.AQL, null, null));

        // Not checking the Jobs API, yet can't figure out how to parse JSON in java.

        // Sleep for 5 milliseconds, because job takes around 15 milliseconds to finish
        Thread.sleep(5);

        // KILL
        System.out.println("\nKilling the cluster now...");
        String KILL_CMD = "ps -ef | awk '/java.*org\\.apache\\.hyracks\\.control\\.[cn]c\\.[CN]CDriver/ {print $2}' | xargs -n 1 kill -9";
        process = new ProcessBuilder("/bin/sh", "-c", KILL_CMD).inheritIO().start();
        Assert.assertEquals(0, process.waitFor());

        // Start Cluster
        process = new ProcessBuilder(joinPath(LOCAL_SAMPLES_DIR, "bin/start-sample-cluster.sh"), "-f").inheritIO().start();
        process.waitFor();
        //Assert.assertEquals(0, process.waitFor());
        Assert.assertEquals(num_records_before_compact, executeQueryAndGetOutput("use dataverse test; count(for $l in dataset LineItem return $l);"));
        int part0_files_after = countFilesEndingWith(new File(DATA_BLUE_PATH),"_b");
        int part1_files_after = countFilesEndingWith(new File(DATA_RED_PATH),"_b");

        System.out.println("\nNumber of files in partition 0 after COMPACT statement: " + part0_files_after);
        System.out.println("\nNumber of files in partition 1 after COMPACT statement: " + part1_files_after);

        Assert.assertEquals(part0_files_before,part0_files_after);
        Assert.assertEquals(part1_files_before,part1_files_after);

    }

    ////////// Helper Methods ///////////////

    private int countFilesEndingWith(File dir, String s) {
        String[] fileNames = dir.list();
        int total = 0;
        for (int i = 0; i < fileNames.length; i++) {
            if (fileNames[i].contains(s)) {
                total++;
            }
        }
        return total;
    }

    private String executeQueryAndGetOutput(String s) throws Exception {
        TestExecutor testExecutor = new TestExecutor();
        InputStream resultStream = testExecutor.executeQuery(s, OutputFormat.ADM,
                new URI("http", null, "127.0.0.1", 19002, Servlets.AQL_QUERY, null, null),
                Collections.emptyList());
        StringWriter sw = new StringWriter();
        IOUtils.copy(resultStream, sw);
        return sw.toString().trim();
    }

}
