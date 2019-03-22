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
package org.apache.asterix.spidersilk;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import me.arminb.spidersilk.SpiderSilkRunner;
import me.arminb.spidersilk.dsl.entities.Deployment;
import me.arminb.spidersilk.exceptions.RuntimeEngineException;

public class SampleTestIT {
    private static final Logger logger = LoggerFactory.getLogger(SampleTestIT.class);

    protected static SpiderSilkRunner runner;

    @BeforeClass
    public static void before() throws RuntimeEngineException {
        Deployment deployment = TestUtil.getSimpleClusterDeployment();
        runner = SpiderSilkRunner.run(deployment);
        TestUtil.waitForClusterToBeUp(runner);
        logger.info("The cluster is UP!");
    }

    @AfterClass
    public static void after() {
        if (runner != null) {
            runner.stop();
        }
    }

    @Test
    public void sampleTest() throws Exception {

        TestExecutor testExecutor = TestUtil.getTestExecutor(runner);
        String ddl = "drop dataverse company if exists;" + "create dataverse company;" + "use company;"
                + "create type Emp as open {" + "  id : int32," + "  name : string" + "};"
                + "create dataset Employee(Emp) primary key id;";

        String insertStatements = "use company;" + "insert into Employee({ \"id\":123,\"name\":\"John Doe\"});";

        String query = "use company;" + "select value emp from Employee emp;";

        testExecutor.executeSqlppUpdateOrDdl(ddl, TestCaseContext.OutputFormat.CLEAN_JSON);
        logger.info("Company dataverse and employee dataset are created!");
        testExecutor.executeSqlppUpdateOrDdl(insertStatements, TestCaseContext.OutputFormat.CLEAN_JSON);
        logger.info("A record is inserted into employee dataset");
        InputStream resultStream = testExecutor.executeSqlppUpdateOrDdl(query, TestCaseContext.OutputFormat.CLEAN_JSON);

        ObjectMapper objectMapper = new ObjectMapper();
        List<Map<String, String>> result = objectMapper.readValue(resultStream, List.class);

        Assert.assertEquals(1, result.size());
        Assert.assertEquals(123, result.get(0).get("id"));
        Assert.assertEquals("John Doe", result.get(0).get("name"));

        logger.info("The fetched record matches the inserted record");
    }
}
