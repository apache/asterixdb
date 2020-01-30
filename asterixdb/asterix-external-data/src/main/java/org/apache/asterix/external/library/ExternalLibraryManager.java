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
package org.apache.asterix.external.library;

import java.io.IOException;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExternalLibraryManager implements ILibraryManager {

    private final Map<Pair<DataverseName, String>, URLClassLoader> libraryClassLoaders = new HashMap<>();
    private static final Logger LOGGER = LogManager.getLogger();

    @Override
    public void registerLibraryClassLoader(DataverseName dataverseName, String libraryName,
            URLClassLoader classLoader) {
        Pair<DataverseName, String> key = getKey(dataverseName, libraryName);
        synchronized (libraryClassLoaders) {
            if (libraryClassLoaders.get(key) != null) {
                return;
            }
            libraryClassLoaders.put(key, classLoader);
        }
    }

    @Override
    public List<Pair<DataverseName, String>> getAllLibraries() {
        ArrayList<Pair<DataverseName, String>> libs = new ArrayList<>();
        synchronized (libraryClassLoaders) {
            libraryClassLoaders.forEach((key, value) -> libs.add(key));
        }
        return libs;
    }

    @Override
    public void deregisterLibraryClassLoader(DataverseName dataverseName, String libraryName) {
        Pair<DataverseName, String> key = getKey(dataverseName, libraryName);
        synchronized (libraryClassLoaders) {
            URLClassLoader cl = libraryClassLoaders.get(key);
            if (cl != null) {
                try {
                    cl.close();
                } catch (IOException e) {
                    LOGGER.error("Unable to close UDF classloader!", e);
                }
                libraryClassLoaders.remove(key);
            }
        }
    }

    @Override
    public ClassLoader getLibraryClassLoader(DataverseName dataverseName, String libraryName) {
        Pair<DataverseName, String> key = getKey(dataverseName, libraryName);
        return libraryClassLoaders.get(key);
    }

    private static Pair<DataverseName, String> getKey(DataverseName dataverseName, String libraryName) {
        return new Pair(dataverseName, libraryName);
    }

}
