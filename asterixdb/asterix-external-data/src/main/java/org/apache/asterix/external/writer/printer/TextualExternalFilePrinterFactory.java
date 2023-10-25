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
package org.apache.asterix.external.writer.printer;

import org.apache.asterix.external.writer.compressor.IExternalFileCompressStreamFactory;
import org.apache.asterix.runtime.writer.IExternalFilePrinter;
import org.apache.asterix.runtime.writer.IExternalFilePrinterFactory;
import org.apache.hyracks.algebricks.data.IPrinterFactory;

public class TextualExternalFilePrinterFactory implements IExternalFilePrinterFactory {
    private static final long serialVersionUID = 9155959967258587588L;
    private final IPrinterFactory printerFactory;
    private final IExternalFileCompressStreamFactory compressStreamFactory;

    public TextualExternalFilePrinterFactory(IPrinterFactory printerFactory,
            IExternalFileCompressStreamFactory compressStreamFactory) {
        this.printerFactory = printerFactory;
        this.compressStreamFactory = compressStreamFactory;
    }

    @Override
    public IExternalFilePrinter createPrinter() {
        return new TextualExternalFilePrinter(printerFactory.createPrinter(), compressStreamFactory);
    }
}
