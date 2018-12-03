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
package org.apache.asterix.drivers;

import java.io.FileReader;

import org.apache.asterix.api.common.AsterixClientConfig;
import org.apache.asterix.api.java.AsterixJavaClient;
import org.apache.asterix.app.translator.DefaultStatementExecutorFactory;
import org.apache.asterix.compiler.provider.AqlCompilationProvider;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.file.StorageComponentProvider;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.ipc.impl.HyracksConnection;
import org.kohsuke.args4j.CmdLineParser;

public class AsterixClientDriver {

    public static void main(String args[]) throws Exception {
        AsterixClientConfig acc = new AsterixClientConfig();
        CmdLineParser cmdParser = new CmdLineParser(acc);
        try {
            cmdParser.parseArgument(args);
        } catch (Exception e) {
            cmdParser.printUsage(System.err);
            throw e;
        }

        if (acc.getArguments().isEmpty()) {
            System.err.println("Please specify the file containing the query.");
            return;
        }
        if (acc.getArguments().size() > 1) {
            System.err.print("Too many arguments. ");
            System.err.println("Only the file contained the query needs to be specified.");
            return;
        }
        boolean exec = new Boolean(acc.execute);
        IHyracksClientConnection hcc = exec ? new HyracksConnection("localhost", acc.hyracksPort) : null;
        AsterixJavaClient q = compileQuery(hcc, acc.getArguments().get(0), new Boolean(acc.optimize), false,
                exec || new Boolean(acc.hyracksJob));
        if (exec) {
            q.execute();
        }
    }

    private static AsterixJavaClient compileQuery(IHyracksClientConnection hcc, String filename, boolean optimize,
            boolean onlyPhysical, boolean createBinaryRuntime) throws Exception {
        ILangCompilationProvider compilationProvider = new AqlCompilationProvider();
        FileReader reader = new FileReader(filename);
        AsterixJavaClient q = new AsterixJavaClient(null, hcc, reader, compilationProvider,
                new DefaultStatementExecutorFactory(), new StorageComponentProvider());
        q.compile(optimize, true, true, true, onlyPhysical, createBinaryRuntime, createBinaryRuntime);
        return q;
    }

}
