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
package edu.uci.ics.hivesterix.test.optimizer;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.junit.Test;

import edu.uci.ics.hivesterix.common.config.ConfUtil;
import edu.uci.ics.hivesterix.test.base.AbstractHivesterixTestCase;

public class OptimizerTestSuiteCaseGenerator extends AbstractHivesterixTestCase {
    private File resultFile;

    OptimizerTestSuiteCaseGenerator(File queryFile, File resultFile) {
        super("testOptimizer", queryFile);
        this.queryFile = queryFile;
        this.resultFile = resultFile;
    }

    @Test
    public void testOptimizer() throws Exception {
        StringBuilder queryString = new StringBuilder();
        readFileToString(queryFile, queryString);
        String[] queries = queryString.toString().split(";");
        StringWriter sw = new StringWriter();

        HiveConf hconf = ConfUtil.getHiveConf();
        Driver driver = new Driver(hconf, new PrintWriter(sw));
        driver.init();

        int i = 0;
        for (String query : queries) {
            if (i == queries.length - 1)
                break;
            if (query.toLowerCase().indexOf("create") >= 0 || query.toLowerCase().indexOf("drop") >= 0
                    || query.toLowerCase().indexOf("set") >= 0 || query.toLowerCase().startsWith("\n\ncreate")
                    || query.toLowerCase().startsWith("\n\ndrop") || query.toLowerCase().startsWith("\n\nset"))
                driver.run(query);
            else
                driver.compile(query);
            driver.clear();
            i++;
        }
        sw.close();
        writeStringToFile(resultFile, sw);
    }
}
