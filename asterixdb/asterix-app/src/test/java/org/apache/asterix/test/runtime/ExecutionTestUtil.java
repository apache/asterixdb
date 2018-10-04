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
package org.apache.asterix.test.runtime;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.IdentitiyResolverFactory;
import org.apache.asterix.testframework.xml.TestGroup;
import org.apache.asterix.testframework.xml.TestSuite;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExecutionTestUtil {

    protected static final Logger LOGGER = LogManager.getLogger();

    protected static final String PATH_ACTUAL = "rttest" + File.separator;

    public static TestGroup FailedGroup;

    public static AsterixHyracksIntegrationUtil integrationUtil = new AsterixHyracksIntegrationUtil();

    public static List<ILibraryManager> setUp(boolean cleanup) throws Exception {
        return setUp(cleanup, AsterixHyracksIntegrationUtil.DEFAULT_CONF_FILE, integrationUtil, true, null);
    }

    public static List<ILibraryManager> setUp(boolean cleanup, String configFile) throws Exception {
        return setUp(cleanup, configFile, integrationUtil, false, null);
    }

    public static List<ILibraryManager> setUp(boolean cleanup, String configFile,
            AsterixHyracksIntegrationUtil alternateIntegrationUtil, boolean startHdfs, List<Pair<IOption, Object>> opts)
            throws Exception {
        System.out.println("Starting setup");
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Starting setup");
        }

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("initializing pseudo cluster");
        }
        integrationUtil = alternateIntegrationUtil;
        if (opts != null) {
            for (Pair<IOption, Object> p : opts) {
                integrationUtil.addOption(p.getLeft(), p.getRight());
            }
        }
        integrationUtil.init(cleanup, configFile);

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("initializing HDFS");
        }

        if (startHdfs) {
            HDFSCluster.getInstance().setup();
        }

        // Set the node resolver to be the identity resolver that expects node
        // names
        // to be node controller ids; a valid assumption in test environment.
        System.setProperty(ExternalDataConstants.NODE_RESOLVER_FACTORY_PROPERTY,
                IdentitiyResolverFactory.class.getName());

        FailedGroup = new TestGroup();
        FailedGroup.setName("failed");

        List<ILibraryManager> libraryManagers = new ArrayList<>();
        // Adds the library manager for CC.
        libraryManagers.add(((ICcApplicationContext) integrationUtil.cc.getApplicationContext()).getLibraryManager());
        // Adds library managers for NCs, one-per-NC.
        for (NodeControllerService nc : integrationUtil.ncs) {
            INcApplicationContext runtimeCtx = (INcApplicationContext) nc.getApplicationContext();
            libraryManagers.add(runtimeCtx.getLibraryManager());
        }
        return libraryManagers;
    }

    public static void tearDown(boolean cleanup) throws Exception {
        tearDown(cleanup, integrationUtil, true);
    }

    public static void tearDown(boolean cleanup, boolean stopHdfs) throws Exception {
        tearDown(cleanup, integrationUtil, stopHdfs);
    }

    public static void tearDown(boolean cleanup, AsterixHyracksIntegrationUtil integrationUtil, boolean stopHdfs)
            throws Exception {
        // validateBufferCacheState(); <-- Commented out until bug is fixed -->
        integrationUtil.deinit(cleanup);
        File outdir = new File(PATH_ACTUAL);
        File[] files = outdir.listFiles();
        if (files == null || files.length == 0) {
            outdir.delete();
        }
        if (stopHdfs) {
            HDFSCluster.getInstance().cleanup();
        }

        if (FailedGroup != null && FailedGroup.getTestCase().size() > 0) {
            File temp = File.createTempFile("failed", ".xml");
            javax.xml.bind.JAXBContext jaxbCtx = null;
            jaxbCtx = javax.xml.bind.JAXBContext.newInstance(TestSuite.class.getPackage().getName());
            javax.xml.bind.Marshaller marshaller = null;
            marshaller = jaxbCtx.createMarshaller();
            marshaller.setProperty(javax.xml.bind.Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
            TestSuite failedSuite = new TestSuite();
            failedSuite.setResultOffsetPath("results");
            failedSuite.setQueryOffsetPath("queries");
            failedSuite.getTestGroup().add(FailedGroup);
            marshaller.marshal(failedSuite, temp);
            System.err.println("The failed.xml is written to :" + temp.getAbsolutePath()
                    + ". You can copy it to only.xml by the following cmd:" + "\rcp " + temp.getAbsolutePath() + " "
                    + Paths.get("./src/test/resources/runtimets/only.xml").toAbsolutePath());
        }
    }

}
