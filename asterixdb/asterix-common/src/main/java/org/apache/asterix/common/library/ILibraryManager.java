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

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.external.ipc.ExternalFunctionResultRouter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.ipc.impl.IPCSystem;

public interface ILibraryManager {

    ILibrary getLibrary(DataverseName dataverseName, String libraryName) throws HyracksDataException;

    void closeLibrary(DataverseName dataverseName, String libraryName) throws HyracksDataException;

    // deployment helpers

    FileReference getLibraryDir(DataverseName dataverseName, String libraryName) throws HyracksDataException;

    FileReference getDistributionDir();

    void dropLibraryPath(FileReference fileRef) throws HyracksDataException;

    byte[] serializeLibraryDescriptor(LibraryDescriptor libraryDescriptor) throws HyracksDataException;

    ExternalFunctionResultRouter getRouter();

    IPCSystem getIPCI();
}
