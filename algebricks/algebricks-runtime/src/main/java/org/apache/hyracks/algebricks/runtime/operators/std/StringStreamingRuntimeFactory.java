/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.algebricks.runtime.operators.std;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public class StringStreamingRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private String command;
    private IPrinterFactory[] printerFactories;
    private char fieldDelimiter;
    private ITupleParserFactory parserFactory;

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
            throws AlgebricksException {
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
                    DumpInStreamToPrintStream disps = new DumpInStreamToPrintStream(process.getErrorStream(),
                            System.err);
                    dumpStderr = new Thread(disps);
                    dumpStderr.start();
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                tAccess.reset(buffer);
                int nTuple = tAccess.getTupleCount();
                for (int t = 0; t < nTuple; t++) {
                    tRef.reset(tAccess, t);
                    for (int i = 0; i < printers.length; i++) {
                        try {
                            printers[i].print(buffer.array(), tRef.getFieldStart(i), tRef.getFieldLength(i), ps);
                        } catch (AlgebricksException e) {
                            throw new HyracksDataException(e);
                        }
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
                    throw new HyracksDataException(e);
                }
                if (ret != 0) {
                    throw new HyracksDataException("Process exit value: " + ret);
                }
                // close the following operator in the chain
                super.close();
            }
        };
    }
}
