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
package org.apache.asterix.app.nc.task;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.asterix.common.api.INCLifecycleTask;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.storage.DatasetResourceReference;
import org.apache.asterix.common.transactions.Checkpoint;
import org.apache.asterix.common.transactions.ICheckpointManager;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.api.service.IControllerService;
import org.apache.hyracks.storage.common.ILocalResourceRepository;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Migrates a legacy storage structure to the current one
 */
public class MigrateStorageResourcesTask implements INCLifecycleTask {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;
    public static final int LEGACY_RESOURCES_TREE_DEPTH_FROM_STORAGE_ROOT = 5;

    @Override
    public void perform(IControllerService cs) throws HyracksDataException {
        final INcApplicationContext appCtx = (INcApplicationContext) cs.getApplicationContext();
        final ICheckpointManager checkpointMgr = appCtx.getTransactionSubsystem().getCheckpointManager();
        final Checkpoint latestCheckpoint = checkpointMgr.getLatest();
        if (latestCheckpoint == null) {
            // nothing to migrate
            return;
        }
        final IIOManager ioManager = appCtx.getIoManager();
        final List<IODeviceHandle> ioDevices = ioManager.getIODevices();
        for (IODeviceHandle ioDeviceHandle : ioDevices) {
            final Path root = Paths.get(ioDeviceHandle.getMount().getAbsolutePath());
            if (!root.toFile().exists()) {
                continue;
            }
            try (Stream<Path> stream = Files.find(root, LEGACY_RESOURCES_TREE_DEPTH_FROM_STORAGE_ROOT,
                    (path, attr) -> path.getFileName().toString().equals(StorageConstants.METADATA_FILE_NAME))) {
                final List<Path> resourceToMigrate = stream.map(Path::getParent).collect(Collectors.toList());
                for (Path src : resourceToMigrate) {
                    final Path dest =
                            migrateResourceMetadata(root.relativize(src), appCtx, latestCheckpoint.getStorageVersion());
                    copyResourceFiles(root.resolve(src), root.resolve(dest),
                            PersistentLocalResourceRepository.INDEX_COMPONENTS);
                    FileUtils.deleteDirectory(root.resolve(src).toFile());
                }
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }
    }

    /**
     * Migrates the resource metadata file at {@code resourcePath} to the new storage structure
     * and updates the migrated resource's metadata to reflect the new path.
     *
     * @param resourcePath
     * @param appCtx
     * @param resourceVersion
     * @return The migrated resource relative path
     * @throws HyracksDataException
     */
    private Path migrateResourceMetadata(Path resourcePath, INcApplicationContext appCtx, int resourceVersion)
            throws HyracksDataException {
        final ILocalResourceRepository localResourceRepository = appCtx.getLocalResourceRepository();
        final LocalResource srcResource = localResourceRepository.get(resourcePath.toFile().getPath());
        final DatasetLocalResource lsmResource = (DatasetLocalResource) srcResource.getResource();
        // recreate the resource with the new path and version
        final DatasetResourceReference lrr = DatasetResourceReference.of(srcResource, resourceVersion);
        final Path destPath = lrr.getRelativePath();
        final FileReference destDir = appCtx.getIoManager().resolve(destPath.toString());
        // ensure the new dest dir is empty
        if (destDir.getFile().exists()) {
            FileUtils.deleteQuietly(destDir.getFile());
        }
        lsmResource.setPath(destPath.toString());

        final LocalResource destResource =
                new LocalResource(srcResource.getId(), srcResource.getVersion(), srcResource.isDurable(), lsmResource);
        LOGGER.info(() -> "Migrating resource from: " + srcResource.getPath() + " to " + destResource.getPath());
        localResourceRepository.insert(destResource);
        return destPath;
    }

    /**
     * Copies the files matching {@code filter} at {@code src} path to {@code dest}
     *
     * @param src
     * @param dest
     * @param filter
     * @throws IOException
     */
    private void copyResourceFiles(Path src, Path dest, Predicate<Path> filter) throws IOException {
        try (Stream<Path> stream = Files.list(src)) {
            final List<Path> srcFiles = stream.filter(filter).collect(Collectors.toList());
            for (Path srcFile : srcFiles) {
                Path fileDest = Paths.get(dest.toString(), srcFile.getFileName().toString());
                Files.copy(srcFile, fileDest);
            }
        }
    }

    @Override
    public String toString() {
        return "{ \"class\" : \"" + getClass().getSimpleName() + "\" }";
    }
}
