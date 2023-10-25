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

import org.apache.asterix.runtime.writer.IExternalFileFilterWriterFactoryProvider;
import org.apache.asterix.runtime.writer.IExternalFilePrinterFactory;
import org.apache.asterix.runtime.writer.IExternalFileWriter;
import org.apache.asterix.runtime.writer.IExternalFileWriterFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;

public final class LocalFSExternalFileWriterFactory implements IExternalFileWriterFactory {
    private static final long serialVersionUID = 871685327574547749L;
    public static final IExternalFileFilterWriterFactoryProvider PROVIDER = c -> new LocalFSExternalFileWriterFactory();

    private LocalFSExternalFileWriterFactory() {
    }

    @Override
    public IExternalFileWriter createWriter(IHyracksTaskContext context, IExternalFilePrinterFactory printerFactory) {
        return new LocalFSExternalFileWriter(printerFactory.createPrinter());
    }

    @Override
    public char getFileSeparator() {
        return File.separatorChar;
    }

    @Override
    public void validate() {
        // NoOp
    }
}
