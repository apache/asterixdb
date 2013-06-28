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
package edu.uci.ics.hivesterix.perf;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.junit.Test;

import edu.uci.ics.hivesterix.common.config.ConfUtil;
import edu.uci.ics.hivesterix.perf.base.AbstractPerfTestCase;

public class PerfTestCase extends AbstractPerfTestCase {
    private File resultFile;
    private FileSystem dfs;

    PerfTestCase(File queryFile, File resultFile) {
        super("testRuntimeFunction", queryFile);
        this.queryFile = queryFile;
        this.resultFile = resultFile;
    }

    @Test
    public void testRuntimeFunction() throws Exception {
        StringBuilder queryString = new StringBuilder();
        readFileToString(queryFile, queryString);
        String[] queries = queryString.toString().split(";");
        StringWriter sw = new StringWriter();

        HiveConf hconf = ConfUtil.getHiveConf();
        Driver driver = new Driver(hconf, new PrintWriter(sw));
        driver.init();

        dfs = FileSystem.get(ConfUtil.getJobConf());

        int i = 0;
        for (String query : queries) {
            if (i == queries.length - 1)
                break;
            driver.run(query);
            driver.clear();
            i++;
        }

        String warehouse = hconf.get("hive.metastore.warehouse.dir");
        String tableName = removeExt(resultFile.getName());
        String directory = warehouse + "/" + tableName + "/";
        String localDirectory = "tmp";

        FileStatus[] files = dfs.listStatus(new Path(directory));
        FileSystem lfs = null;
        if (files == null) {
            lfs = FileSystem.getLocal(ConfUtil.getJobConf());
            files = lfs.listStatus(new Path(directory));
        }

        File resultDirectory = new File(localDirectory + "/" + tableName);
        deleteDir(resultDirectory);
        resultDirectory.mkdir();

        for (FileStatus fs : files) {
            Path src = fs.getPath();
            if (src.getName().indexOf("crc") >= 0)
                continue;

            String destStr = localDirectory + "/" + tableName + "/" + src.getName();
            Path dest = new Path(destStr);
            if (lfs != null) {
                lfs.copyToLocalFile(src, dest);
                dfs.copyFromLocalFile(dest, new Path(directory));
            } else
                dfs.copyToLocalFile(src, dest);
        }

        File[] rFiles = resultDirectory.listFiles();
        StringBuilder sb = new StringBuilder();
        for (File r : rFiles) {
            if (r.getName().indexOf("crc") >= 0)
                continue;
            readFileToString(r, sb);
        }
        deleteDir(resultDirectory);

        StringBuilder buf = new StringBuilder();
        readFileToString(resultFile, buf);
        if (!equal(buf, sb)) {
            throw new Exception("Result for " + queryFile + " changed:\n" + sw.toString());
        }
    }

    private void deleteDir(File resultDirectory) {
        if (resultDirectory.exists()) {
            File[] rFiles = resultDirectory.listFiles();
            for (File r : rFiles)
                r.delete();
            resultDirectory.delete();
        }
    }

    private boolean equal(StringBuilder sb1, StringBuilder sb2) {
        String s1 = sb1.toString();
        String s2 = sb2.toString();
        String[] rowsOne = s1.split("\n");
        String[] rowsTwo = s2.split("\n");

        if (rowsOne.length != rowsTwo.length)
            return false;

        for (int i = 0; i < rowsOne.length; i++) {
            String row1 = rowsOne[i];
            String row2 = rowsTwo[i];

            if (row1.equals(row2))
                continue;

            String[] fields1 = row1.split("");
            String[] fields2 = row2.split("");

            for (int j = 0; j < fields1.length; j++) {
                if (fields1[j].equals(fields2[j])) {
                    continue;
                } else if (fields1[j].indexOf('.') < 0) {
                    return false;
                } else {
                    Float float1 = Float.parseFloat(fields1[j]);
                    Float float2 = Float.parseFloat(fields2[j]);

                    if (Math.abs(float1 - float2) == 0)
                        continue;
                    else
                        return false;
                }
            }
        }

        return true;
    }
}
