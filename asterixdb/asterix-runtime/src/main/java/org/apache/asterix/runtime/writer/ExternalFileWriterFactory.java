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
package org.apache.asterix.runtime.writer;

import org.apache.hyracks.algebricks.runtime.writers.IExternalWriter;
import org.apache.hyracks.algebricks.runtime.writers.IExternalWriterFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ExternalFileWriterFactory implements IExternalWriterFactory {
    private static final long serialVersionUID = 1412969574113419638L;
    private final IExternalFileWriterFactory writerFactory;
    private final IExternalPrinterFactory printerFactory;
    private final int maxResult;
    private final IPathResolverFactory pathResolverFactory;

    public ExternalFileWriterFactory(IExternalFileWriterFactory writerFactory, IExternalPrinterFactory printerFactory,
            IPathResolverFactory pathResolverFactory, int maxResult) {
        this.writerFactory = writerFactory;
        this.printerFactory = printerFactory;
        this.pathResolverFactory = pathResolverFactory;
        this.maxResult = maxResult;
    }

    @Override
    public IExternalWriter createWriter(IHyracksTaskContext context) throws HyracksDataException {
        IPathResolver resolver = pathResolverFactory.createResolver(context);
        IExternalFileWriter writer = writerFactory.createWriter(context, printerFactory);
        return new ExternalFileWriter(resolver, writer, maxResult);
    }
}
