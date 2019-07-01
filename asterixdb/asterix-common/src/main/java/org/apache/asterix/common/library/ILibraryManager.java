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

package org.apache.asterix.common.library;

import java.net.URLClassLoader;
import java.util.List;

import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface ILibraryManager {

    /**
     * Registers the library class loader with the external library manager.
     * <code>dataverseName</code> and <code>libraryName</code> uniquely identifies a class loader.
     * @param libraryName
     * @param classLoader
     */
    void registerLibraryClassLoader(String dataverseName, String libraryName, URLClassLoader classLoader)
            throws HyracksDataException;

    /**
     * @return all registered libraries.
     */
    List<Pair<String, String>> getAllLibraries();

    /**
     * De-registers a library class loader.
     *
     * @param dataverseName
     * @param libraryName
     */
    void deregisterLibraryClassLoader(String dataverseName, String libraryName);

    /**
     * Finds a class loader for a given pair of dataverse name and library name.
     *
     * @param dataverseName
     * @param libraryName
     * @return the library class loader associated with the dataverse and library.
     */
    ClassLoader getLibraryClassLoader(String dataverseName, String libraryName);

    /**
     * Add function parameters  to library manager if it exists.
     * @param dataverseName
     * @param fullFunctionName
     * @param parameters
     */

    void addFunctionParameters(String dataverseName, String fullFunctionName, List<String> parameters);

    /**
     * Get a list of parameters.
     * @param dataverseName
     * @param fullFunctionName
     * @return A list contains all pre-specified function parameters.
     */
    List<String> getFunctionParameters(String dataverseName, String fullFunctionName);
}
