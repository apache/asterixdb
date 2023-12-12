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
package org.apache.asterix.external.library;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.external.ipc.ExternalFunctionResultRouter;
import org.apache.asterix.external.ipc.PythonTCPSocketProto;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.ipc.impl.IPCSystem;

public class PythonLibraryTCPSocketEvaluator extends AbstractLibrarySocketEvaluator {

    public static final String ENTRYPOINT = "entrypoint.py";
    public static final String SITE_PACKAGES = "site-packages";

    private Process p;
    private ILibraryManager libMgr;
    private File pythonHome;
    private ExternalFunctionResultRouter router;
    private IPCSystem ipcSys;
    private String sitePkgs;
    private List<String> pythonArgs;
    private Map<String, String> pythonEnv;

    public PythonLibraryTCPSocketEvaluator(JobId jobId, PythonLibraryEvaluatorId evaluatorId, ILibraryManager libMgr,
            File pythonHome, String sitePkgs, List<String> pythonArgs, Map<String, String> pythonEnv,
            ExternalFunctionResultRouter router, IPCSystem ipcSys, TaskAttemptId task,
            IWarningCollector warningCollector, SourceLocation sourceLoc) {
        super(jobId, evaluatorId, task, warningCollector, sourceLoc);
        this.libMgr = libMgr;
        this.pythonHome = pythonHome;
        this.sitePkgs = sitePkgs;
        this.pythonArgs = pythonArgs;
        this.pythonEnv = pythonEnv;
        this.router = router;
        this.ipcSys = ipcSys;
    }

    @Override
    public void start() throws IOException, AsterixException {
        PythonLibraryEvaluatorId fnId = (PythonLibraryEvaluatorId) id;
        PythonLibrary library = (PythonLibrary) libMgr.getLibrary(fnId.getLibraryNamespace(), fnId.getLibraryName());
        String wd = library.getFile().getAbsolutePath();
        int port = ipcSys.getSocketAddress().getPort();
        List<String> args = new ArrayList<>();
        args.add(pythonHome.getAbsolutePath());
        args.addAll(pythonArgs);
        args.add(ENTRYPOINT);
        args.add(InetAddress.getLoopbackAddress().getHostAddress());
        args.add(Integer.toString(port));
        args.add(sitePkgs);
        ProcessBuilder pb = new ProcessBuilder(args.toArray(new String[0]));
        pb.environment().putAll(pythonEnv);
        pb.directory(new File(wd));
        p = pb.start();
        proto = new PythonTCPSocketProto(p.getOutputStream(), router, p);
        proto.start();
        proto.helo();
    }

    @Override
    public void deallocate() {
        if (p != null) {
            boolean dead = false;
            try {
                p.destroy();
                dead = p.waitFor(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                //gonna kill it anyway
            }
            if (!dead) {
                p.destroyForcibly();
            }
        }
        router.removeRoute(proto.getRouteId());
    }

    static PythonLibraryTCPSocketEvaluator getInstance(IExternalFunctionInfo finfo, ILibraryManager libMgr,
            ExternalFunctionResultRouter router, IPCSystem ipcSys, File pythonHome, IHyracksTaskContext ctx,
            String sitePkgs, List<String> pythonArgs, Map<String, String> pythonEnv, IWarningCollector warningCollector,
            SourceLocation sourceLoc) throws IOException, AsterixException {
        PythonLibraryEvaluatorId evaluatorId = new PythonLibraryEvaluatorId(finfo.getLibraryNamespace(),
                finfo.getLibraryName(), Thread.currentThread());
        PythonLibraryTCPSocketEvaluator evaluator = (PythonLibraryTCPSocketEvaluator) ctx.getStateObject(evaluatorId);
        if (evaluator == null) {
            evaluator = new PythonLibraryTCPSocketEvaluator(ctx.getJobletContext().getJobId(), evaluatorId, libMgr,
                    pythonHome, sitePkgs, pythonArgs, pythonEnv, router, ipcSys, ctx.getTaskAttemptId(),
                    warningCollector, sourceLoc);
            ctx.getJobletContext().registerDeallocatable(evaluator);
            evaluator.start();
            ctx.setStateObject(evaluator);
        }
        return evaluator;
    }

}
