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

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.net.ProtocolFamily;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.external.ipc.PythonDomainSocketProto;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PythonLibraryDomainSocketEvaluator extends AbstractLibrarySocketEvaluator {

    private final ILibraryManager libMgr;
    private final Path sockPath;
    SocketChannel chan;
    ProcessHandle pid;
    private static final Logger LOGGER = LogManager.getLogger(ExternalLibraryManager.class);

    public PythonLibraryDomainSocketEvaluator(JobId jobId, PythonLibraryEvaluatorId evaluatorId, ILibraryManager libMgr,
            TaskAttemptId task, IWarningCollector warningCollector, SourceLocation sourceLoc, Path sockPath) {
        super(jobId, evaluatorId, task, warningCollector, sourceLoc);
        this.libMgr = libMgr;
        this.sockPath = sockPath;
    }

    public void start() throws IOException, AsterixException {
        PythonLibraryEvaluatorId fnId = (PythonLibraryEvaluatorId) id;
        PythonLibrary library = (PythonLibrary) libMgr.getLibrary(fnId.getLibraryNamespace(), fnId.getLibraryName());
        String wd = library.getFile().getAbsolutePath();
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        SocketAddress sockAddr;
        try {
            VarHandle sockEnum = lookup.in(StandardProtocolFamily.class)
                    .findStaticVarHandle(StandardProtocolFamily.class, "UNIX", StandardProtocolFamily.class);
            Class domainSock = Class.forName("java.net.UnixDomainSocketAddress");
            MethodType unixDomainSockAddrType = MethodType.methodType(domainSock, Path.class);
            MethodHandle unixDomainSockAddr = lookup.findStatic(domainSock, "of", unixDomainSockAddrType);
            MethodType sockChanMethodType = MethodType.methodType(SocketChannel.class, ProtocolFamily.class);
            MethodHandle sockChanOpen = lookup.findStatic(SocketChannel.class, "open", sockChanMethodType);
            sockAddr = ((SocketAddress) unixDomainSockAddr.invoke(sockPath));
            chan = (SocketChannel) sockChanOpen.invoke(sockEnum.get());
        } catch (Throwable e) {
            throw HyracksDataException.create(ErrorCode.LOCAL_NETWORK_ERROR, e);
        }
        chan.connect(sockAddr);
        proto = new PythonDomainSocketProto(Channels.newOutputStream(chan), chan, wd);
        proto.start();
        proto.helo();
        this.pid = ((PythonDomainSocketProto) proto).getPid();
    }

    @Override
    public void deallocate() {
        try {
            if (proto != null) {
                proto.quit();
            }
            if (chan != null) {
                chan.close();
            }
        } catch (IOException e) {
            LOGGER.error("Caught exception exiting Python UDF:", e);
        }
        if (pid != null && pid.isAlive()) {
            LOGGER.error("Python UDF " + pid.pid() + " did not exit as expected.");
        }
    }

    static PythonLibraryDomainSocketEvaluator getInstance(IExternalFunctionInfo finfo, ILibraryManager libMgr,
            IHyracksTaskContext ctx, IWarningCollector warningCollector, SourceLocation sourceLoc)
            throws IOException, AsterixException {
        PythonLibraryEvaluatorId evaluatorId = new PythonLibraryEvaluatorId(finfo.getLibraryNamespace(),
                finfo.getLibraryName(), Thread.currentThread());
        PythonLibraryDomainSocketEvaluator evaluator =
                (PythonLibraryDomainSocketEvaluator) ctx.getStateObject(evaluatorId);
        if (evaluator == null) {
            Path sockPath = Path.of(ctx.getJobletContext().getServiceContext().getAppConfig()
                    .getString(NCConfig.Option.PYTHON_DS_PATH));
            evaluator = new PythonLibraryDomainSocketEvaluator(ctx.getJobletContext().getJobId(), evaluatorId, libMgr,
                    ctx.getTaskAttemptId(), warningCollector, sourceLoc, sockPath);
            ctx.getJobletContext().registerDeallocatable(evaluator);
            evaluator.start();
            ctx.setStateObject(evaluator);
        }
        return evaluator;
    }

}
