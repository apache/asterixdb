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
package org.apache.hyracks.algebricks.runtime.operators.std;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.ByteBuffer;

import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.file.ITupleParser;
import org.apache.hyracks.dataflow.std.file.ITupleParserFactory;

public class StringStreamingRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private String command;
    private IPrinterFactory[] printerFactories;
    private char fieldDelimiter;
    private ITupleParserFactory parserFactory;

    /*
     * NOTE: This operator doesn't follow the IFrameWriter protocol
     */
    public StringStreamingRuntimeFactory(String command, IPrinterFactory[] printerFactories, char fieldDelimiter,
            ITupleParserFactory parserFactory) {
        super(null);
        this.command = command;
        this.printerFactories = printerFactories;
        this.fieldDelimiter = fieldDelimiter;
        this.parserFactory = parserFactory;
    }

    @Override
    public AbstractOneInputOneOutputOneFramePushRuntime createOneOutputPushRuntime(final IHyracksTaskContext ctx)
            throws HyracksDataException {
        final IPrinter[] printers = new IPrinter[printerFactories.length];
        for (int i = 0; i < printerFactories.length; i++) {
            printers[i] = printerFactories[i].createPrinter();
        }

        return new AbstractOneInputOneOutputOneFramePushRuntime() {
            final class ForwardScriptOutput implements Runnable {

                private InputStream inStream;
                private ITupleParser parser;

                public ForwardScriptOutput(ITupleParser parser, InputStream inStream) {
                    this.parser = parser;
                    this.inStream = inStream;
                }

                @Override
                public void run() {
                    try {
                        parser.parse(inStream, writer);
                    } catch (HyracksDataException e) {
                        throw new RuntimeException(e);
                    } finally {
                        try {
                            inStream.close();
                        } catch (Exception e) {
                        }
                    }
                }
            }

            final class DumpInStreamToPrintStream implements Runnable {

                private BufferedReader reader;
                private PrintStream printStream;

                public DumpInStreamToPrintStream(InputStream inStream, PrintStream printStream) {
                    this.reader = new BufferedReader(new InputStreamReader(inStream));
                    this.printStream = printStream;
                }

                @Override
                public void run() {
                    String s;
                    try {
                        while ((s = reader.readLine()) != null) {
                            printStream.println(s);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } finally {
                        try {
                            reader.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        printStream.close();
                    }
                }

            }

            private Process process;
            private PrintStream ps;
            private boolean first = true;
            private Thread outputPipe;
            private Thread dumpStderr;

            @Override
            public void open() throws HyracksDataException {
                if (first) {
                    first = false;
                    initAccessAppendRef(ctx);
                }
                try {
                    ITupleParser parser = parserFactory.createTupleParser(ctx);
                    process = Runtime.getRuntime().exec(command);
                    ps = new PrintStream(process.getOutputStream());
                    ForwardScriptOutput fso = new ForwardScriptOutput(parser, process.getInputStream());
                    outputPipe = new Thread(fso);
                    outputPipe.start();
                    DumpInStreamToPrintStream disps =
                            new DumpInStreamToPrintStream(process.getErrorStream(), System.err);
                    dumpStderr = new Thread(disps);
                    dumpStderr.start();
                    super.open();
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                tAccess.reset(buffer);
                int nTuple = tAccess.getTupleCount();
                for (int t = 0; t < nTuple; t++) {
                    tRef.reset(tAccess, t);
                    for (int i = 0; i < printers.length; i++) {
                        printers[i].print(buffer.array(), tRef.getFieldStart(i), tRef.getFieldLength(i), ps);
                        ps.print(fieldDelimiter);
                        if (i == printers.length - 1) {
                            ps.print('\n');
                        }
                    }
                }
            }

            @Override
            public void close() throws HyracksDataException {
                // first close the printer printing to the process
                ps.close();
                int ret = 0;
                // then wait for the process to finish

                try {
                    ret = process.waitFor();
                    outputPipe.join();
                    dumpStderr.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw HyracksDataException.create(e);
                }
                if (ret != 0) {
                    throw new HyracksDataException("Process exit value: " + ret);
                }
                // close the following operator in the chain
                super.close();
            }

            @Override
            public void flush() throws HyracksDataException {
                ps.flush();
            }
        };
    }
}
