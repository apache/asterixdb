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

package org.apache.hyracks.storage.am.lsm.common.api;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;

/**
 * Provides file names for LSM on-disk components. Also cleans up invalid files.
 * There are separate methods to get file names for merge and flush because we
 * need to guarantee the correct order of on-disk components (i.e., the
 * components produced by flush are always newer than those produced by a
 * merge).
 */
public interface ILSMIndexFileManager {
    void createDirs() throws HyracksDataException;

    void deleteDirs() throws HyracksDataException;

    LSMComponentFileReferences getRelFlushFileReference() throws HyracksDataException;

    LSMComponentFileReferences getRelMergeFileReference(String firstFileName, String lastFileName)
            throws HyracksDataException;

    FileReference getBaseDir();

    // Deletes invalid files, and returns list of valid files from baseDir.
    // The returned valid files are correctly sorted (based on the recency of data).
    List<LSMComponentFileReferences> cleanupAndGetValidFiles() throws HyracksDataException;

    Comparator<String> getFileNameComparator();

    /**
     * @return delete existing transaction disk component file reference
     * @throws HyracksDataException
     */
    void deleteTransactionFiles() throws HyracksDataException;

    /**
     * Rename files of a transaction removing the transaction prefix and return the component file reference in order to be committed
     *
     * @return the renamed component file references
     * @throws HyracksDataException
     */
    LSMComponentFileReferences getTransactionFileReferenceForCommit() throws HyracksDataException;

    /**
     * Recover transaction files without returning them
     *
     * @throws HyracksDataException
     */
    void recoverTransaction() throws HyracksDataException;

    /**
     * @return a reference to the transaction disk component file reference
     * @throws IOException
     */
    LSMComponentFileReferences getNewTransactionFileReference() throws IOException;
}
