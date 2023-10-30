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

import java.io.File;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.runtime.writer.ExternalFileWriterConfiguration;
import org.apache.asterix.runtime.writer.IExternalFileFilterWriterFactoryProvider;
import org.apache.asterix.runtime.writer.IExternalFilePrinterFactory;
import org.apache.asterix.runtime.writer.IExternalFileWriter;
import org.apache.asterix.runtime.writer.IExternalFileWriterFactory;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;

public final class LocalFSExternalFileWriterFactory implements IExternalFileWriterFactory {
    private static final long serialVersionUID = 871685327574547749L;
    private static final char SEPARATOR = File.separatorChar;
    public static final IExternalFileFilterWriterFactoryProvider PROVIDER =
            new IExternalFileFilterWriterFactoryProvider() {
                @Override
                public IExternalFileWriterFactory create(ExternalFileWriterConfiguration configuration) {
                    return new LocalFSExternalFileWriterFactory(configuration);
                }

                @Override
                public char getSeparator() {
                    return SEPARATOR;
                }
            };
    private static final ILocalFSValidator NO_OP_VALIDATOR = LocalFSExternalFileWriterFactory::noOpValidation;
    private static final ILocalFSValidator VALIDATOR = LocalFSExternalFileWriterFactory::validate;
    private final SourceLocation pathSourceLocation;
    private final boolean singleNodeCluster;
    private final String staticPath;
    private boolean validated;

    private LocalFSExternalFileWriterFactory(ExternalFileWriterConfiguration externalConfig) {
        pathSourceLocation = externalConfig.getPathSourceLocation();
        singleNodeCluster = externalConfig.isSingleNodeCluster();
        staticPath = externalConfig.getStaticPath();
        validated = false;
    }

    @Override
    public IExternalFileWriter createWriter(IHyracksTaskContext context, IExternalFilePrinterFactory printerFactory)
            throws HyracksDataException {
        ILocalFSValidator validator = VALIDATOR;
        if (staticPath != null) {
            synchronized (this) {
                validateStaticPath();
            }
            validator = NO_OP_VALIDATOR;
        }
        return new LocalFSExternalFileWriter(printerFactory.createPrinter(), validator, pathSourceLocation);
    }

    @Override
    public char getSeparator() {
        return SEPARATOR;
    }

    @Override
    public void validate() throws AlgebricksException {
        // A special case validation for a single node cluster
        if (singleNodeCluster && staticPath != null) {
            if (isNonEmptyDirectory(new File(staticPath))) {
                throw new CompilationException(ErrorCode.DIRECTORY_IS_NOT_EMPTY, pathSourceLocation, staticPath);
            }

            // Ensure that it is not validated again in a single node cluster
            validated = true;
        }
    }

    private void validateStaticPath() throws HyracksDataException {
        if (validated) {
            return;
        }

        if (isNonEmptyDirectory(new File(staticPath))) {
            throw new RuntimeDataException(ErrorCode.DIRECTORY_IS_NOT_EMPTY, pathSourceLocation, staticPath);
        }
        validated = true;
    }

    static boolean isNonEmptyDirectory(File parentDirectory) {
        if (parentDirectory.exists()) {
            String[] files = parentDirectory.list();
            return files != null && files.length != 0;
        }

        return false;
    }

    private static void noOpValidation(String directory, SourceLocation sourceLocation) {
        // NoOp
    }

    private static void validate(String directory, SourceLocation sourceLocation) throws HyracksDataException {
        if (isNonEmptyDirectory(new File(directory))) {
            throw new RuntimeDataException(ErrorCode.DIRECTORY_IS_NOT_EMPTY, sourceLocation, directory);
        }
    }
}
