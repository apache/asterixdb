/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package org.apache.asterix.external.library;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.asterix.common.functions.ExternalFunctionLanguage;
import org.apache.asterix.common.library.ILibrary;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JavaLibrary implements ILibrary {

    private final ExternalLibraryClassLoader cl;

    private static final Logger LOGGER = LogManager.getLogger();

    public JavaLibrary(File libDir) throws HyracksDataException, MalformedURLException {

        FilenameFilter jarFileFilter = (dir, name) -> name.endsWith(".jar");

        // Get the jar file <Allow only a single jar file>
        String[] jarsInLibDir = libDir.list(jarFileFilter);
        if (jarsInLibDir.length > 1) {
            throw new HyracksDataException("Incorrect library structure: found multiple library jars");
        }
        if (jarsInLibDir.length <= 0) {
            throw new HyracksDataException("Incorrect library structure: could not find library jar");
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

        ClassLoader parentClassLoader = JavaLibrary.class.getClassLoader();
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

        // create and return the class loader
        this.cl = new ExternalLibraryClassLoader(urls, parentClassLoader);

    }

    @Override
    public ExternalFunctionLanguage getLanguage() {
        return ExternalFunctionLanguage.JAVA;
    }

    public ClassLoader getClassLoader() {
        return cl;
    }

    public void close() {
        try {
            if (cl != null) {
                cl.close();
            }
        } catch (IOException e) {
            LOGGER.error("Couldn't close classloader", e);
        }
    }

}
