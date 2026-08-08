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

package org.apache.hyracks.storage.am.lsm.vector.impls;

import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.impls.IndexComponentFileReference;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMVTreeComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * File manager for LSM Vector Clustering Trees.
 *
 * This class manages the files associated with LSM Vector Clustering Tree components,
 * including naming conventions, file creation, and cleanup operations for both
 * in-memory and disk components.
 */
public class LSMVTreeFileManager extends AbstractLSMIndexFileManager {
    private static final Logger LOGGER = LogManager.getLogger();

    private static final String VCTREE_SUFFIX = "vct";
    private static final String STATIC_STRUCTURE_SUFFIX = ".staticstructure";

    private final TreeIndexFactory<? extends ITreeIndex> vTreeFactory;

    private static final FilenameFilter vTreeFilter =
            (dir, name) -> !name.startsWith(".") && name.endsWith(VCTREE_SUFFIX);

    public LSMVTreeFileManager(IIOManager ioManager, FileReference file,
            TreeIndexFactory<? extends ITreeIndex> vTreeFactory) {
        super(ioManager, file, null);
        this.vTreeFactory = vTreeFactory;
    }

    @Override
    public LSMComponentFileReferences getRelFlushFileReference() throws HyracksDataException {
        String baseName = getNextComponentSequence(vTreeFilter);
        return new LSMVTreeComponentFileReferences(baseDir.getChild(baseName + DELIMITER + VCTREE_SUFFIX), null, null,
                baseDir.getChild(STATIC_STRUCTURE_SUFFIX));
    }

    @Override
    public LSMComponentFileReferences getRelMergeFileReference(String firstFileName, String lastFileName) {
        final String baseName = IndexComponentFileReference.getMergeSequence(firstFileName, lastFileName);
        // The static structure is shared by every component and is never rewritten by a merge, so the merged
        // component references the same shared file the flush path references — there is no per-merge
        // ".staticstructure" file, and nothing ever creates one. It goes in the dedicated static-structure
        // slot, not the bloom-filter slot.
        return new LSMVTreeComponentFileReferences(baseDir.getChild(baseName + DELIMITER + VCTREE_SUFFIX), null, null,
                baseDir.getChild(STATIC_STRUCTURE_SUFFIX));
    }

    @Override
    public List<LSMComponentFileReferences> cleanupAndGetValidFiles() throws HyracksDataException {
        List<LSMComponentFileReferences> validFiles = new ArrayList<>();
        ArrayList<IndexComponentFileReference> allVTreeFiles = new ArrayList<>();

        // Gather all VTree data component files.
        collectVTreeFiles(allVTreeFiles);

        // The single shared static structure (one per index) is required to read any component; without a
        // valid one no VTree component can be opened, so drop everything. Note that this branch DELETES every
        // data component, so it must only be taken when the static structure is *known* to be absent — an
        // I/O error while probing is propagated by validateStaticStructureFile() instead (see there).
        FileReference staticStructureFile = baseDir.getChild(STATIC_STRUCTURE_SUFFIX);
        if (!validateStaticStructureFile(staticStructureFile)) {
            LOGGER.log(Level.TRACE, "Invalid or missing shared .staticstructure file: {}",
                    staticStructureFile.getAbsolutePath());
            for (IndexComponentFileReference vTreeFile : allVTreeFiles) {
                cleanupOrphanedVTreeFile(vTreeFile.getFileRef());
            }
            return validFiles; // Return empty list
        }

        if (allVTreeFiles.isEmpty()) {
            return validFiles;
        }

        // Drop (and delete) any component whose sequence interval is contained in a more-recent (merged)
        // component. A merge writes the merged component and then deletes its inputs as a SEPARATE step; a
        // crash in between leaves the pre-merge inputs on disk. Loading both the merged component and its
        // inputs would duplicate every record and resurface deletes the merge reconciled away. Mirrors
        // AbstractLSMIndexFileManager.cleanupAndGetValidFiles (isMoreRecentThan / isWithin). Natural order
        // sorts by sequenceStart asc then sequenceEnd desc, so a merged component precedes the flushed
        // inputs it contains and becomes the running "widest" reference.
        Collections.sort(allVTreeFiles);
        ArrayList<IndexComponentFileReference> survivors = new ArrayList<>();
        IndexComponentFileReference last = allVTreeFiles.get(0);
        survivors.add(last);
        for (int i = 1; i < allVTreeFiles.size(); i++) {
            IndexComponentFileReference current = allVTreeFiles.get(i);
            if (current.isMoreRecentThan(last)) {
                survivors.add(current);
                last = current;
            } else if (current.isWithin(last)) {
                LOGGER.log(Level.TRACE, "Deleting merged-away VTree component {} (within {})", current.getSequence(),
                        last.getSequence());
                cleanupOrphanedVTreeFile(current.getFileRef());
            } else {
                throw HyracksDataException.create(ErrorCode.FOUND_OVERLAPPING_LSM_FILES, baseDir);
            }
        }

        // LSM expects disk components ordered newest -> oldest: [2,2], [1,1], [0,0].
        survivors.sort(Collections.reverseOrder());
        for (IndexComponentFileReference vTreeFile : survivors) {
            LOGGER.log(Level.TRACE, "Valid VTree component: {} (using shared .staticstructure)",
                    vTreeFile.getSequence());
            // Shared static structure goes in its own slot (LSMVTreeComponentFileReferences), not the
            // bloom-filter slot — a VTree component has no bloom filter.
            validFiles
                    .add(new LSMVTreeComponentFileReferences(vTreeFile.getFileRef(), null, null, staticStructureFile));
        }
        return validFiles;
    }

    @Override
    protected boolean areHolesAllowed() {
        return false; // VTree components must be contiguous
    }

    /**
     * Validates and returns the shared static structure file reference.
     * The static structure is shared across all LSM components and contains
     * the hierarchical k-means clustering metadata.
     *
     * @return LSMVTreeComponentFileReferences with only the static structure, or null if invalid
     * @throws HyracksDataException if validation fails
     */
    public LSMVTreeComponentFileReferences getStaticStructureFileReference() throws HyracksDataException {
        FileReference staticStructureFile = baseDir.getChild(STATIC_STRUCTURE_SUFFIX);
        if (validateStaticStructureFile(staticStructureFile)) {
            return new LSMVTreeComponentFileReferences(null, null, null, staticStructureFile);
        }
        return null;
    }

    /**
     * Lists all VTree component files in the base directory and appends their
     * IndexComponentFileReference forms to the supplied list.
     * <p>
     * Goes through {@link IIOManager} rather than {@code baseDir.getFile().list(...)}: on cloud deployments a
     * component that exists for the resource need not have a local file, so a direct {@code java.io.File}
     * listing under-reports (and would silently drop valid components here).
     */
    private void collectVTreeFiles(List<IndexComponentFileReference> files) throws HyracksDataException {
        for (FileReference fileRef : ioManager.list(baseDir, vTreeFilter)) {
            files.add(IndexComponentFileReference.of(fileRef));
        }
    }

    /**
     * Validates that a .staticstructure file exists and is valid.
     * <p>
     * Returns {@code false} only for a file that is <em>definitely</em> absent. An I/O failure while probing
     * ("could not determine") is propagated, deliberately: {@link #cleanupAndGetValidFiles()} reads
     * {@code false} as "this index has no static structure" and deletes every data component, so swallowing a
     * transient failure here (a cloud hiccup, EMFILE, a slow mount) would destroy an otherwise healthy index
     * instead of failing activation.
     *
     * @param staticStructureFile The .staticstructure file to validate
     * @return true if the file exists and is valid, false if it is known not to exist
     * @throws HyracksDataException if existence could not be determined
     */
    private boolean validateStaticStructureFile(FileReference staticStructureFile) throws HyracksDataException {
        if (!ioManager.exists(staticStructureFile)) {
            LOGGER.log(Level.TRACE, "Static structure file does not exist: {}", staticStructureFile.getAbsolutePath());
            return false;
        }
        LOGGER.log(Level.TRACE, "Static structure file is valid: {}", staticStructureFile.getAbsolutePath());
        return true;
    }

    /**
     * Cleans up an orphaned VTree file when its .staticstructure file is missing or invalid.
     *
     * @param vTreeFile The orphaned VTree file to clean up
     */
    private void cleanupOrphanedVTreeFile(FileReference vTreeFile) {
        try {
            LOGGER.log(Level.TRACE, "Cleaning up orphaned VTree file: {}", vTreeFile.getAbsolutePath());
            ioManager.delete(vTreeFile);
        } catch (Exception e) {
            LOGGER.log(Level.TRACE, "Failed to clean up orphaned VTree file {}: {}", vTreeFile.getAbsolutePath(),
                    e.getMessage());
        }
    }
}
