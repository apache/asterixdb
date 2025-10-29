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
package org.apache.asterix.api.http.server;

import static org.apache.asterix.external.library.ExternalLibraryManager.DESCRIPTOR_FILE_NAME;
import static org.apache.asterix.external.library.ExternalLibraryManager.writeDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.api.IRequestReference;
import org.apache.asterix.common.functions.ExternalFunctionLanguage;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.library.LibraryDescriptor;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.external.util.ExternalLibraryUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;

import io.netty.buffer.ByteBufInputStream;

public class NCUdfDSApiServlet extends AbstractNCUdfServlet {

    public NCUdfDSApiServlet(ConcurrentMap<String, Object> ctx, String[] paths, IApplicationContext appCtx) {
        super(ctx, paths, appCtx);
    }

    protected void distributeLibrary(LibraryUploadData uploadData, DataverseName libDv, String libName, String fileExt,
            ExternalFunctionLanguage language, MessageDigest digest, Namespace namespace,
            IRequestReference requestReference, IServletRequest request) throws Exception {
        writeLibToCloud(uploadData, namespace, libName, digest, language);
        doCreate(namespace, libName, language, ExternalLibraryUtils.digestToHexString(digest), null, true,
                getSysAuthHeader(), requestReference, request);
    }

    private void writeLibToCloud(LibraryUploadData uploadData, Namespace libNamespace, String libName,
            MessageDigest digest, ExternalFunctionLanguage language) throws IOException {
        FileReference libDir = libraryManager.getLibraryDir(libNamespace, libName);
        IIOManager cloudIoMgr = libraryManager.getCloudIOManager();
        FileReference lib = libDir.getChild(ILibraryManager.LIBRARY_ARCHIVE_NAME);
        if (!libDir.getFile().exists()) {
            Files.createDirectories(lib.getFile().toPath().getParent());
        }
        if (!lib.getFile().exists()) {
            Files.createFile(lib.getFile().toPath());
        }
        IFileHandle fh = cloudIoMgr.open(lib, IIOManager.FileReadWriteMode.READ_WRITE,
                IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
        WritableByteChannel outChannel = cloudIoMgr.newWritableChannel(fh);
        byte[] writeBuf = new byte[4096];
        FileReference targetDescFile = libDir.getChild(DESCRIPTOR_FILE_NAME);
        try (OutputStream outputStream = new DigestOutputStream(Channels.newOutputStream(outChannel), digest);
                InputStream ui = new ByteBufInputStream((uploadData.fileUpload).getByteBuf())) {
            IOUtils.copyLarge(ui, outputStream, writeBuf);
            outputStream.flush();
            cloudIoMgr.sync(fh, true);
            writeDescriptor(libraryManager, targetDescFile,
                    new LibraryDescriptor(language, ExternalLibraryUtils.digestToHexString(digest)), true, writeBuf);
        } finally {
            cloudIoMgr.close(fh);
        }
    }

    protected String getSysAuthHeader() {
        return sysAuthHeader;
    }

    @Override
    protected boolean isRequestPermitted(IServletRequest request, IServletResponse response) throws IOException {
        return true;
    }

    @Override
    protected void post(IServletRequest request, IServletResponse response) throws IOException {
        if (isRequestPermitted(request, response)) {
            handleModification(request, response, LibraryOperation.UPSERT);
        }
    }

    @Override
    protected void delete(IServletRequest request, IServletResponse response) throws IOException {
        if (isRequestPermitted(request, response)) {
            handleModification(request, response, LibraryOperation.DELETE);
        }
    }

}
