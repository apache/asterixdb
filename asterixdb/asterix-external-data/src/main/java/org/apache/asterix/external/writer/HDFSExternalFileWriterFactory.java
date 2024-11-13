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
package org.apache.asterix.external.writer;

import static org.apache.asterix.common.exceptions.ErrorCode.EXTERNAL_SOURCE_ERROR;
import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.UUID;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.HDFSUtils;
import org.apache.asterix.runtime.writer.ExternalFileWriterConfiguration;
import org.apache.asterix.runtime.writer.IExternalFileWriter;
import org.apache.asterix.runtime.writer.IExternalFileWriterFactory;
import org.apache.asterix.runtime.writer.IExternalFileWriterFactoryProvider;
import org.apache.asterix.runtime.writer.IExternalPrinter;
import org.apache.asterix.runtime.writer.IExternalPrinterFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.util.ExceptionUtils;

public class HDFSExternalFileWriterFactory implements IExternalFileWriterFactory {
    private static final long serialVersionUID = 1L;
    private static final char SEPARATOR = '/';
    public static final IExternalFileWriterFactoryProvider PROVIDER = new IExternalFileWriterFactoryProvider() {
        @Override
        public IExternalFileWriterFactory create(ExternalFileWriterConfiguration configuration) {
            return new HDFSExternalFileWriterFactory(configuration);
        }

        @Override
        public char getSeparator() {
            return SEPARATOR;
        }
    };

    private final Map<String, String> configuration;
    private final String staticPath;
    private final SourceLocation pathSourceLocation;
    private transient Credentials credentials;
    private byte[] serializedCredentials;
    private transient FileSystem fs;

    private HDFSExternalFileWriterFactory(ExternalFileWriterConfiguration externalConfig) {
        configuration = externalConfig.getConfiguration();
        staticPath = HDFSUtils.updateRootPath(externalConfig.getStaticPath(), false);
        pathSourceLocation = externalConfig.getPathSourceLocation();
    }

    private FileSystem createFileSystem(Configuration conf) throws CompilationException, HyracksDataException {
        try {
            if (credentials != null) {
                UserGroupInformation ugi = UserGroupInformation.createRemoteUser(UUID.randomUUID().toString());
                ugi.addCredentials(credentials);
                return ugi.doAs((PrivilegedExceptionAction<FileSystem>) () -> FileSystem.get(conf));
            }
            return FileSystem.get(conf);
        } catch (InterruptedException ex) {
            throw HyracksDataException.create(ex);
        } catch (IOException ex) {
            throw CompilationException.create(EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
        }
    }

    private void buildFileSystem() throws HyracksDataException {
        try {
            synchronized (this) {
                if (credentials == null && serializedCredentials != null) {
                    credentials = new Credentials();
                    HDFSUtils.deserialize(serializedCredentials, credentials);
                }
                if (fs == null) {
                    Configuration conf = HDFSUtils.configureHDFSwrite(configuration);
                    fs = createFileSystem(conf);
                }
            }
        } catch (IOException | CompilationException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public IExternalFileWriter createWriter(IHyracksTaskContext context, IExternalPrinterFactory printerFactory)
            throws HyracksDataException {
        buildFileSystem();
        IExternalPrinter printer = printerFactory.createPrinter();
        return new HDFSExternalFileWriter(printer, fs, staticPath == null, pathSourceLocation);
    }

    @Override
    public char getSeparator() {
        return SEPARATOR;
    }

    @Override
    public void validate() throws AlgebricksException {
        Configuration conf = HDFSUtils.configureHDFSwrite(configuration);
        credentials = HDFSUtils.configureHadoopAuthentication(configuration, conf);
        try {
            if (credentials != null) {
                serializedCredentials = HDFSUtils.serialize(credentials);
            }
            try (FileSystem testFs = createFileSystem(conf)) {
                doValidate(testFs);
            }
        } catch (IOException ex) {
            throw CompilationException.create(ErrorCode.EXTERNAL_SINK_ERROR, ExceptionUtils.getMessageOrToString(ex));
        }
    }

    private void doValidate(FileSystem testFs) throws IOException, AlgebricksException {
        if (staticPath != null) {
            Path dirPath = new Path(staticPath);
            if (testFs.exists(dirPath)) {
                FileStatus fileStatus = testFs.getFileStatus(dirPath);
                if (fileStatus.isFile()) {
                    throw new CompilationException(ErrorCode.DIRECTORY_IS_NOT_EMPTY, pathSourceLocation, staticPath);
                }
                if (fileStatus.isDirectory()) {
                    FileStatus[] fileStatuses = testFs.listStatus(dirPath);
                    if (fileStatuses.length != 0) {
                        throw new CompilationException(ErrorCode.DIRECTORY_IS_NOT_EMPTY, pathSourceLocation,
                                staticPath);
                    }
                }
            }
            checkDirectoryWritePermission(testFs);
        }
    }

    private void checkDirectoryWritePermission(FileSystem fs) throws AlgebricksException {
        if (!Boolean.parseBoolean(configuration.getOrDefault(ExternalDataConstants.KEY_VALIDATE_WRITE_PERMISSION,
                Boolean.TRUE.toString()))) {
            return;
        }
        Path path = new Path(staticPath, "testFile");
        try {
            try (FSDataOutputStream outputStream = fs.create(path)) {
                fs.deleteOnExit(path);
                outputStream.write(0);
            }
        } catch (IOException ex) {
            throw CompilationException.create(ErrorCode.EXTERNAL_SINK_ERROR, ex, getMessageOrToString(ex));
        }
    }
}
