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

import static org.apache.asterix.external.library.ExternalLibraryManager.DESCRIPTOR_FILE_NAME;
import static org.apache.hyracks.control.common.controllers.NCConfig.Option.PYTHON_USE_BUNDLED_MSGPACK;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.asterix.common.functions.ExternalFunctionLanguage;
import org.apache.asterix.common.library.LibraryDescriptor;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.external.library.ExternalLibraryManager;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.util.file.FileUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LibraryDeployPrepareOperatorDescriptor extends AbstractLibraryOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private static final int DOWNLOAD_RETRY_COUNT = 10;

    private static final Logger LOGGER = LogManager.getLogger(LibraryDeployPrepareOperatorDescriptor.class);

    private final ExternalFunctionLanguage language;
    private final URI libLocation;
    private final String authToken;

    public LibraryDeployPrepareOperatorDescriptor(IOperatorDescriptorRegistry spec, DataverseName dataverseName,
            String libraryName, ExternalFunctionLanguage language, URI libLocation, String authToken) {
        super(spec, dataverseName, libraryName);
        this.language = language;
        this.libLocation = libLocation;
        this.authToken = authToken;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new AbstractLibraryNodePushable(ctx) {

            private byte[] copyBuffer;

            @Override
            protected void execute() throws IOException {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Prepare deployment of library {}.{}", dataverseName, libraryName);
                }

                // #. create library dir if necessary, clean 'stage' dir
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
                download(targetFile);

                // extract from the archive
                FileReference contentsDir = stageDir.getChild(ExternalLibraryManager.CONTENTS_DIR_NAME);
                mkdir(contentsDir);

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Extracting library from {} into {}", targetFile, contentsDir);
                }

                switch (language) {
                    case JAVA:
                        if (!LibraryDescriptor.FILE_EXT_ZIP.equals(fileExt)) {
                            // shouldn't happen
                            throw new IOException("Unexpected file type: " + fileExt);
                        }
                        unzip(targetFile, contentsDir);
                        break;
                    case PYTHON:
                        if (!LibraryDescriptor.FILE_EXT_PYZ.equals(fileExt)) {
                            // shouldn't happen
                            throw new IOException("Unexpected file type: " + fileExt);
                        }
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
                writeDescriptor(targetDescFile, new LibraryDescriptor(language));

                flushDirectory(contentsDir);
                flushDirectory(stageDir);
            }

            private void download(FileReference targetFile) throws HyracksException {
                try {
                    targetFile.getFile().createNewFile();
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
                IFileHandle fHandle = ioManager.open(targetFile, IIOManager.FileReadWriteMode.READ_WRITE,
                        IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
                try {
                    CloseableHttpClient httpClient = HttpClientBuilder.create().build();
                    try {
                        // retry 10 times at maximum for downloading binaries
                        HttpGet request = new HttpGet(libLocation);
                        request.setHeader(HttpHeaders.AUTHORIZATION, authToken);
                        int tried = 0;
                        Exception trace = null;
                        while (tried < DOWNLOAD_RETRY_COUNT) {
                            tried++;
                            CloseableHttpResponse response = null;
                            try {
                                response = httpClient.execute(request);
                                if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                                    throw new IOException("Http Error: " + response.getStatusLine().getStatusCode());
                                }
                                HttpEntity e = response.getEntity();
                                if (e == null) {
                                    throw new IOException("No response");
                                }
                                WritableByteChannel outChannel = ioManager.newWritableChannel(fHandle);
                                OutputStream outStream = Channels.newOutputStream(outChannel);
                                e.writeTo(outStream);
                                outStream.flush();
                                ioManager.sync(fHandle, true);
                                return;
                            } catch (IOException e) {
                                LOGGER.error("Unable to download library", e);
                                trace = e;
                                try {
                                    ioManager.truncate(fHandle, 0);
                                } catch (IOException e2) {
                                    throw HyracksDataException.create(e2);
                                }
                            } finally {
                                if (response != null) {
                                    try {
                                        response.close();
                                    } catch (IOException e) {
                                        LOGGER.warn("Failed to close", e);
                                    }
                                }
                            }
                        }

                        throw HyracksDataException.create(trace);
                    } finally {
                        try {
                            httpClient.close();
                        } catch (IOException e) {
                            LOGGER.warn("Failed to close", e);
                        }
                    }
                } finally {
                    try {
                        ioManager.close(fHandle);
                    } catch (HyracksDataException e) {
                        LOGGER.warn("Failed to close", e);
                    }
                }
            }

            private void unzip(FileReference sourceFile, FileReference outputDir) throws IOException {
                boolean logTraceEnabled = LOGGER.isTraceEnabled();
                Set<Path> newDirs = new HashSet<>();
                Path outputDirPath = outputDir.getFile().toPath().toAbsolutePath().normalize();
                try (ZipFile zipFile = new ZipFile(sourceFile.getFile())) {
                    Enumeration<? extends ZipEntry> entries = zipFile.entries();
                    while (entries.hasMoreElements()) {
                        ZipEntry entry = entries.nextElement();
                        if (entry.isDirectory()) {
                            continue;
                        }
                        Path entryOutputPath = outputDirPath.resolve(entry.getName()).toAbsolutePath().normalize();
                        if (!entryOutputPath.startsWith(outputDirPath)) {
                            throw new IOException("Malformed ZIP archive: " + entry.getName());
                        }
                        Path entryOutputDir = entryOutputPath.getParent();
                        Files.createDirectories(entryOutputDir);
                        // remember new directories so we can flush them later
                        for (Path p = entryOutputDir; !p.equals(outputDirPath); p = p.getParent()) {
                            newDirs.add(p);
                        }
                        try (InputStream in = zipFile.getInputStream(entry)) {
                            FileReference entryOutputFileRef =
                                    ioManager.resolveAbsolutePath(entryOutputPath.toString());
                            if (logTraceEnabled) {
                                LOGGER.trace("Extracting file {}", entryOutputFileRef);
                            }
                            writeAndForce(entryOutputFileRef, in);
                        }
                    }
                }
                for (Path newDir : newDirs) {
                    flushDirectory(newDir);
                }
            }

            private void shiv(FileReference sourceFile, FileReference stageDir, FileReference contentsDir,
                    boolean writeMsgpack) throws IOException {
                FileReference msgpack = stageDir.getChild("msgpack.pyz");
                if (writeMsgpack) {
                    writeShim(msgpack, writeMsgpack);
                    File msgPackFolder = new File(contentsDir.getRelativePath(), "ipc");
                    FileReference msgPackFolderRef =
                            new FileReference(contentsDir.getDeviceHandle(), msgPackFolder.getPath());
                    unzip(msgpack, msgPackFolderRef);
                    Files.delete(msgpack.getFile().toPath());
                }
                unzip(sourceFile, contentsDir);
                writeShim(contentsDir.getChild("entrypoint.py"), false);
            }

            private boolean writeShim(FileReference outputFile, boolean optional) throws IOException {
                InputStream is = getClass().getClassLoader().getResourceAsStream(outputFile.getFile().getName());
                if (is == null) {
                    throw new IOException("Classpath does not contain necessary Python resources!");
                }
                try {
                    writeAndForce(outputFile, is);
                } finally {
                    is.close();
                }
                return true;
            }

            private void writeDescriptor(FileReference descFile, LibraryDescriptor desc) throws IOException {
                byte[] bytes = libraryManager.serializeLibraryDescriptor(desc);
                writeAndForce(descFile, new ByteArrayInputStream(bytes));
            }

            private void writeAndForce(FileReference outputFile, InputStream dataStream) throws IOException {
                outputFile.getFile().createNewFile();
                IFileHandle fHandle = ioManager.open(outputFile, IIOManager.FileReadWriteMode.READ_WRITE,
                        IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
                try {
                    WritableByteChannel outChannel = ioManager.newWritableChannel(fHandle);
                    OutputStream outputStream = Channels.newOutputStream(outChannel);
                    IOUtils.copyLarge(dataStream, outputStream, getCopyBuffer());
                    outputStream.flush();
                    ioManager.sync(fHandle, true);
                } finally {
                    ioManager.close(fHandle);
                }
            }

            private byte[] getCopyBuffer() {
                if (copyBuffer == null) {
                    copyBuffer = new byte[4096];
                }
                return copyBuffer;
            }
        };
    }
}
