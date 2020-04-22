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
package org.apache.asterix.app.external;

import static org.apache.asterix.api.http.server.UdfApiServlet.UDF_RESPONSE_TIMEOUT;
import static org.apache.asterix.api.http.server.UdfApiServlet.makeDeploymentId;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.app.message.DeleteUdfMessage;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExternalLibraryUtils {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final FilenameFilter nonHiddenFileNameFilter = (dir, name) -> !name.startsWith(".");

    private ExternalLibraryUtils() {
    }

    public static void setUpExternaLibrary(ILibraryManager externalLibraryManager, String libraryPath)
            throws Exception {
        // get the installed library dirs
        String[] parts = libraryPath.split(File.separator);
        DataverseName catenatedDv = DataverseName.createFromCanonicalForm(parts[parts.length - 1]);
        String libraryName = catenatedDv.getParts().get(catenatedDv.getParts().size() - 1);
        DataverseName dvName = DataverseName.create(catenatedDv.getParts(), 0, catenatedDv.getParts().size() - 1);
        registerClassLoader(externalLibraryManager, dvName, libraryName, libraryPath);
    }

    public static void setUpInstalledLibraries(ILibraryManager externalLibraryManager, File appDir) throws Exception {
        File[] libs = appDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return dir.isDirectory();
            }
        });
        if (libs != null) {
            for (File lib : libs) {
                setUpExternaLibrary(externalLibraryManager, lib.getAbsolutePath());
            }
        }
    }

    public static void deleteDeployedUdf(ICCMessageBroker broker, ICcApplicationContext appCtx,
            DataverseName dataverseName, String lib) throws Exception {
        long reqId = broker.newRequestId();
        List<INcAddressedMessage> requests = new ArrayList<>();
        List<String> ncs = new ArrayList<>(appCtx.getClusterStateManager().getParticipantNodes());
        ncs.forEach(s -> requests.add(new DeleteUdfMessage(dataverseName, lib, reqId)));
        broker.sendSyncRequestToNCs(reqId, ncs, requests, UDF_RESPONSE_TIMEOUT);
        appCtx.getLibraryManager().deregisterLibraryClassLoader(dataverseName, lib);
        appCtx.getHcc().unDeployBinary(new DeploymentId(makeDeploymentId(dataverseName, lib)));
    }

    /**
     * register the library class loader with the external library manager
     *
     * @param dataverse
     * @param libraryPath
     * @throws Exception
     */
    protected static void registerClassLoader(ILibraryManager externalLibraryManager, DataverseName dataverse,
            String name, String libraryPath) throws Exception {
        // get the class loader
        URLClassLoader classLoader = getLibraryClassLoader(dataverse, name, libraryPath);
        // register it with the external library manager
        externalLibraryManager.registerLibraryClassLoader(dataverse, name, classLoader);
    }

    /**
     * Get the class loader for the library
     *
     * @param dataverse
     * @param libraryPath
     * @return
     * @throws Exception
     */
    private static URLClassLoader getLibraryClassLoader(DataverseName dataverse, String name, String libraryPath)
            throws Exception {
        // Get a reference to the library directory
        File installDir = new File(libraryPath);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Installing lirbary " + name + " in dataverse " + dataverse + "." + " Install Directory: "
                    + installDir.getAbsolutePath());
        }

        // get a reference to the specific library dir
        File libDir = installDir;

        FilenameFilter jarFileFilter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".jar");
            }
        };

        // Get the jar file <Allow only a single jar file>
        String[] jarsInLibDir = libDir.list(jarFileFilter);
        if (jarsInLibDir.length > 1) {
            throw new Exception("Incorrect library structure: found multiple library jars");
        }
        if (jarsInLibDir.length <= 0) {
            throw new Exception("Incorrect library structure: could not find library jar");
        }

        File libJar = new File(libDir, jarsInLibDir[0]);
        // get the jar dependencies
        File libDependencyDir = new File(libDir.getAbsolutePath() + File.separator + "lib");
        int numDependencies = 1;
        String[] libraryDependencies = null;
        if (libDependencyDir.exists()) {
            libraryDependencies = libDependencyDir.list(jarFileFilter);
            numDependencies += libraryDependencies.length;
        }

        ClassLoader parentClassLoader = ExternalLibraryUtils.class.getClassLoader();
        URL[] urls = new URL[numDependencies];
        int count = 0;
        // get url of library
        urls[count++] = libJar.toURI().toURL();

        // get urls for dependencies
        if (libraryDependencies != null && libraryDependencies.length > 0) {
            for (String dependency : libraryDependencies) {
                File file = new File(libDependencyDir + File.separator + dependency);
                urls[count++] = file.toURI().toURL();
            }
        }

        if (LOGGER.isInfoEnabled()) {
            StringBuilder logMesg = new StringBuilder("Classpath for library " + dataverse + ": ");
            for (URL url : urls) {
                logMesg.append(url.getFile() + File.pathSeparatorChar);
            }
            LOGGER.info(logMesg.toString());
        }

        // create and return the class loader
        return new ExternalLibraryClassLoader(urls, parentClassLoader);
    }

    /**
     * @return the directory "System.getProperty("app.home", System.getProperty("user.home")/lib/udfs/uninstall"
     */
    protected static File getLibraryUninstallDir() {
        return new File(System.getProperty("app.home", System.getProperty("user.home")) + File.separator + "lib"
                + File.separator + "udfs" + File.separator + "uninstall");
    }

    public static String getExternalFunctionFullName(String libraryName, String functionName) {
        return libraryName + "#" + functionName;
    }

}
