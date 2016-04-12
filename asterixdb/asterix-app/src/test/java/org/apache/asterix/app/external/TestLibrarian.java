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

import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.List;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.event.service.AsterixEventServiceUtil;
import org.apache.asterix.external.library.ExternalLibraryManager;
import org.apache.asterix.test.aql.ITestLibrarian;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class TestLibrarian implements ITestLibrarian {

    public static final String LIBRARY_DIR_NAME = "library";

    @Override
    public void install(String dvName, String libName, String libPath) throws Exception {
        // get the directory of the to be installed libraries
        File installLibDir = ExternalLibraryUtils.getLibraryInstallDir();
        // directory exists?
        if (!installLibDir.exists()) {
            installLibDir.mkdir();
        }
        // copy the library file into the directory
        File destinationDir = new File(
                installLibDir.getAbsolutePath() + File.separator + dvName + File.separator + libName);
        FileUtils.deleteQuietly(destinationDir);
        destinationDir.mkdirs();
        try {
            AsterixEventServiceUtil.unzip(libPath, destinationDir.getAbsolutePath());
        } catch (Exception e) {

            throw new Exception("Couldn't unzip the file: " + libPath, e);
        }
        // for each file (library), register library
        ExternalLibraryUtils.registerLibrary(dvName, libName, true, destinationDir);
        // get library file
        // install if needed (add functions, adapters, datasources, parsers to the metadata)
        // <Not required for use>
        ExternalLibraryUtils.installLibraryIfNeeded(dvName, destinationDir, new HashMap<>());
    }

    @Override
    public void uninstall(String dvName, String libName) throws RemoteException, AsterixException, ACIDException {
        ExternalLibraryUtils.uninstallLibrary(dvName, libName);
        ExternalLibraryManager.deregisterLibraryClassLoader(dvName, libName);
    }

    public static void removeLibraryDir() throws IOException {
        File installLibDir = ExternalLibraryUtils.getLibraryInstallDir();
        if (!installLibDir.getAbsolutePath().endsWith(LIBRARY_DIR_NAME)) {
            throw new HyracksDataException("Invalid library directory");
        }
        FileUtils.deleteQuietly(installLibDir);
    }

    public static void cleanup() throws AsterixException, RemoteException, ACIDException {
        List<Pair<String, String>> libs = ExternalLibraryManager.getAllLibraries();
        for (Pair<String, String> dvAndLib : libs) {
            ExternalLibraryUtils.uninstallLibrary(dvAndLib.first, dvAndLib.second);
            ExternalLibraryManager.deregisterLibraryClassLoader(dvAndLib.first, dvAndLib.second);
        }
    }
}
