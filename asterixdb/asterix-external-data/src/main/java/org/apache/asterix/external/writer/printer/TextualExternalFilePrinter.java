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

import java.io.OutputStream;
import java.io.PrintStream;

import org.apache.asterix.external.writer.compressor.IExternalFileCompressStreamFactory;
import org.apache.asterix.runtime.writer.IExternalFilePrinter;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

final class TextualExternalFilePrinter implements IExternalFilePrinter {
    private final IPrinter printer;
    private final IExternalFileCompressStreamFactory compressStreamFactory;
    private TextualOutputStreamDelegate delegate;
    private PrintStream printStream;

    TextualExternalFilePrinter(IPrinter printer, IExternalFileCompressStreamFactory compressStreamFactory) {
        this.printer = printer;
        this.compressStreamFactory = compressStreamFactory;
    }

    @Override
    public void open() throws HyracksDataException {
        printer.init();
    }

    @Override
    public void newStream(OutputStream outputStream) throws HyracksDataException {
        if (printStream != null) {
            close();
        }
        delegate = new TextualOutputStreamDelegate(compressStreamFactory.createStream(outputStream));
        printStream = new PrintStream(delegate);
    }

    @Override
    public void print(IValueReference value) throws HyracksDataException {
        printer.print(value.getByteArray(), value.getStartOffset(), value.getLength(), printStream);
        printStream.println();
        delegate.checkError();
    }

    @Override
    public void close() throws HyracksDataException {
        if (printStream != null) {
            printStream.close();
            printStream = null;
            delegate.checkError();
            delegate = null;
        }
    }
}
