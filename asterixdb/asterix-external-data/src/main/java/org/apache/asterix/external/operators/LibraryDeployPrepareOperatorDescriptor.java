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

package org.apache.asterix.external.operators;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.apache.asterix.common.library.ILibraryManager.LIBRARY_ARCHIVE_NAME;
import static org.apache.asterix.external.library.ExternalLibraryManager.CONTENTS_DIR_NAME;
import static org.apache.asterix.external.library.ExternalLibraryManager.DESCRIPTOR_FILE_NAME;
import static org.apache.asterix.external.library.ExternalLibraryManager.REV_1_DIR_NAME;
import static org.apache.asterix.external.library.ExternalLibraryManager.writeDescriptor;
import static org.apache.asterix.external.library.PythonLibraryTCPSocketEvaluator.ENTRYPOINT;
import static org.apache.hyracks.control.common.controllers.NCConfig.Option.PYTHON_USE_BUNDLED_MSGPACK;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Collections;

import org.apache.asterix.common.functions.ExternalFunctionLanguage;
import org.apache.asterix.common.library.LibraryDescriptor;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.external.util.ExternalLibraryUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.util.file.FileUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LibraryDeployPrepareOperatorDescriptor extends AbstractLibraryOperatorDescriptor {

    private static final long serialVersionUID = 2L;

    private static final Logger LOGGER = LogManager.getLogger(LibraryDeployPrepareOperatorDescriptor.class);

    private final ExternalFunctionLanguage language;
    private final URI libLocation;
    private final String authToken;

    public LibraryDeployPrepareOperatorDescriptor(IOperatorDescriptorRegistry spec, Namespace namespace,
            String libraryName, ExternalFunctionLanguage language, URI libLocation, String authToken) {
        super(spec, namespace, libraryName);
        this.language = language;
        this.libLocation = libLocation;
        this.authToken = authToken;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new AbstractLibraryNodePushable(ctx) {

            private final byte[] copyBuf = new byte[4096];

            private void cloudDeploy() throws IOException {
                FileReference libDir = getLibraryDir();
                libDir = new FileReference(libDir.getDeviceHandle(), FilenameUtils.normalize(libDir.getRelativePath()));
                cloudIoManager.downloadLibrary(Collections.singletonList(libDir));
                FileReference content = libDir.getChild(REV_1_DIR_NAME).getChild(CONTENTS_DIR_NAME);
                libraryManager.unzip(libDir.getChild(LIBRARY_ARCHIVE_NAME), content);
                libraryManager.writeShim(content.getChild(ENTRYPOINT), copyBuf);
                Files.copy(libDir.getChild(DESCRIPTOR_FILE_NAME).getFile().toPath(),
                        content.getParent().getChild(DESCRIPTOR_FILE_NAME).getFile().toPath(), REPLACE_EXISTING);
            }

            @Override
            protected void execute() throws IOException {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Prepare deployment of library {}.{}", namespace, libraryName);
                }

                if (libLocation == null && cloudMode) {
                    cloudDeploy();
                    return;
                }

                //#. create library dir if necessary, clean 'stage' dir

                FileReference libDir = getLibraryDir();
                Path libDirPath = libDir.getFile().toPath();

                FileReference stage = getStageDir();
                if (Files.isDirectory(libDirPath)) {
                    dropIfExists(stage);
                } else {
                    dropIfExists(libDir);
                    FileUtil.forceMkdirs(libDir.getFile());
                    Path dataverseDir = libDirPath.getParent();
                    flushDirectory(dataverseDir); // might've created this dir
                    flushDirectory(dataverseDir.getParent()); // might've created this dir
                }
                mkdir(stage);

                // #. download new content into 'stage' dir
                fetch(stage);

                // #. close the library (close its open files if any)
                closeLibrary();

                // #. if 'rev_1' dir exists then rename 'rev_1' dir to 'rev_0' dir.
                FileReference rev1 = getRev1Dir();
                if (rev1.getFile().exists()) {
                    FileReference rev0 = getRev0Dir();
                    move(rev1, rev0);
                }

                // #. flush library dir
                flushDirectory(libDir);
            }

            private void fetch(FileReference stageDir) throws IOException {

                String libLocationPath = libLocation.getPath();
                String fileExt = FilenameUtils.getExtension(libLocationPath);

                FileReference targetFile = stageDir.getChild("lib." + fileExt);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Downloading library from {} into {}", libLocation, targetFile);
                }
                MessageDigest digest = libraryManager.download(targetFile, authToken, libLocation);
                // extract from the archive
                FileReference contentsDir = stageDir.getChild(CONTENTS_DIR_NAME);
                mkdir(contentsDir);

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Extracting library from {} into {}", targetFile, contentsDir);
                }

                switch (language) {
                    case JAVA:
                        libraryManager.unzip(targetFile, contentsDir);
                        break;
                    case PYTHON:
                        boolean extractMsgPack = ctx.getJobletContext().getServiceContext().getAppConfig()
                                .getBoolean(PYTHON_USE_BUNDLED_MSGPACK);
                        shiv(targetFile, stageDir, contentsDir, extractMsgPack);
                        break;
                    default:
                        // shouldn't happen
                        throw new IOException("Unexpected language: " + language);
                }

                // write library descriptor
                FileReference targetDescFile = stageDir.getChild(DESCRIPTOR_FILE_NAME);
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Writing library descriptor into {}", targetDescFile);
                }
                writeDescriptor(libraryManager, targetDescFile,
                        new LibraryDescriptor(language, ExternalLibraryUtils.digestToHexString(digest)), false,
                        copyBuf);

                flushDirectory(contentsDir);
                flushDirectory(stageDir);
            }

            private void shiv(FileReference sourceFile, FileReference stageDir, FileReference contentsDir,
                    boolean writeMsgpack) throws IOException {
                FileReference msgpack = stageDir.getChild("msgpack.pyz");
                if (writeMsgpack) {
                    libraryManager.writeShim(msgpack, copyBuf);
                    File msgPackFolder = new File(contentsDir.getRelativePath(), "ipc");
                    FileReference msgPackFolderRef =
                            new FileReference(contentsDir.getDeviceHandle(), msgPackFolder.getPath());
                    libraryManager.unzip(msgpack, msgPackFolderRef);
                    Files.delete(msgpack.getFile().toPath());
                }
                libraryManager.unzip(sourceFile, contentsDir);
                libraryManager.writeShim(contentsDir.getChild(ENTRYPOINT), copyBuf);
            }

        };
    }
}
