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

import static org.apache.asterix.common.exceptions.ErrorCode.EXTERNAL_UDF_EXCEPTION;
import static org.msgpack.core.MessagePack.Code.ARRAY16;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.external.ipc.ExternalFunctionResultRouter;
import org.apache.asterix.external.ipc.PythonIPCProto;
import org.apache.asterix.external.library.msgpack.MessagePackerFromADM;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.resources.IDeallocatable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.ipc.impl.IPCSystem;

public class PythonLibraryEvaluator extends AbstractStateObject implements IDeallocatable {

    public static final String ENTRYPOINT = "entrypoint.py";
    public static final String SITE_PACKAGES = "site-packages";

    private Process p;
    private ILibraryManager libMgr;
    private File pythonHome;
    private PythonIPCProto proto;
    private ExternalFunctionResultRouter router;
    private IPCSystem ipcSys;
    private String sitePkgs;
    private List<String> pythonArgs;
    private Map<String, String> pythonEnv;
    private TaskAttemptId task;
    private IWarningCollector warningCollector;
    private SourceLocation sourceLoc;

    public PythonLibraryEvaluator(JobId jobId, PythonLibraryEvaluatorId evaluatorId, ILibraryManager libMgr,
            File pythonHome, String sitePkgs, List<String> pythonArgs, Map<String, String> pythonEnv,
            ExternalFunctionResultRouter router, IPCSystem ipcSys, TaskAttemptId task,
            IWarningCollector warningCollector, SourceLocation sourceLoc) {
        super(jobId, evaluatorId);
        this.libMgr = libMgr;
        this.pythonHome = pythonHome;
        this.sitePkgs = sitePkgs;
        this.pythonArgs = pythonArgs;
        this.pythonEnv = pythonEnv;
        this.router = router;
        this.task = task;
        this.ipcSys = ipcSys;
        this.warningCollector = warningCollector;
        this.sourceLoc = sourceLoc;

    }

    private void initialize() throws IOException, AsterixException {
        PythonLibraryEvaluatorId fnId = (PythonLibraryEvaluatorId) id;
        PythonLibrary library =
                (PythonLibrary) libMgr.getLibrary(fnId.getLibraryDataverseName(), fnId.getLibraryName());
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
        proto = new PythonIPCProto(p.getOutputStream(), router, p);
        proto.start();
        proto.helo();
    }

    public long initialize(IExternalFunctionInfo finfo) throws IOException, AsterixException {
        List<String> externalIdents = finfo.getExternalIdentifier();
        String packageModule = externalIdents.get(0);
        String clazz;
        String fn;
        String externalIdent1 = externalIdents.get(1);
        int idx = externalIdent1.lastIndexOf('.');
        if (idx >= 0) {
            clazz = externalIdent1.substring(0, idx);
            fn = externalIdent1.substring(idx + 1);
        } else {
            clazz = null;
            fn = externalIdent1;
        }
        return proto.init(packageModule, clazz, fn);
    }

    public ByteBuffer callPython(long id, ByteBuffer arguments, int numArgs) throws IOException {
        ByteBuffer ret = null;
        try {
            ret = proto.call(id, arguments, numArgs);
        } catch (AsterixException e) {
            if (warningCollector.shouldWarn()) {
                warningCollector.warn(Warning.of(sourceLoc, EXTERNAL_UDF_EXCEPTION, e.getMessage()));
            }
        }
        return ret;
    }

    public ByteBuffer callPythonMulti(long id, ByteBuffer arguments, int numTuples) throws IOException {
        ByteBuffer ret = null;
        try {
            ret = proto.callMulti(id, arguments, numTuples);
        } catch (AsterixException e) {
            if (warningCollector.shouldWarn()) {
                warningCollector.warn(Warning.of(sourceLoc, EXTERNAL_UDF_EXCEPTION, e.getMessage()));
            }
        }
        return ret;
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

    public static ATypeTag setArgument(IAType type, IValueReference valueReference, ByteBuffer argHolder,
            boolean nullCall) throws IOException {
        ATypeTag tag = type.getTypeTag();
        if (tag == ATypeTag.ANY) {
            TaggedValuePointable pointy = TaggedValuePointable.FACTORY.createPointable();
            pointy.set(valueReference);
            ATypeTag rtTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(pointy.getTag());
            IAType rtType = TypeTagUtil.getBuiltinTypeByTag(rtTypeTag);
            return MessagePackerFromADM.pack(valueReference, rtType, argHolder, nullCall);
        } else {
            return MessagePackerFromADM.pack(valueReference, type, argHolder, nullCall);
        }
    }

    public static ATypeTag peekArgument(IAType type, IValueReference valueReference) throws HyracksDataException {
        ATypeTag tag = type.getTypeTag();
        if (tag == ATypeTag.ANY) {
            TaggedValuePointable pointy = TaggedValuePointable.FACTORY.createPointable();
            pointy.set(valueReference);
            ATypeTag rtTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(pointy.getTag());
            IAType rtType = TypeTagUtil.getBuiltinTypeByTag(rtTypeTag);
            return MessagePackerFromADM.peekUnknown(rtType);
        } else {
            return MessagePackerFromADM.peekUnknown(type);
        }
    }

    public static void setVoidArgument(ByteBuffer argHolder) {
        argHolder.put(ARRAY16);
        argHolder.putShort((short) 0);
    }

    public static PythonLibraryEvaluator getInstance(IExternalFunctionInfo finfo, ILibraryManager libMgr,
            ExternalFunctionResultRouter router, IPCSystem ipcSys, File pythonHome, IHyracksTaskContext ctx,
            String sitePkgs, List<String> pythonArgs, Map<String, String> pythonEnv, IWarningCollector warningCollector,
            SourceLocation sourceLoc) throws IOException, AsterixException {
        PythonLibraryEvaluatorId evaluatorId = new PythonLibraryEvaluatorId(finfo.getLibraryDataverseName(),
                finfo.getLibraryName(), Thread.currentThread());
        PythonLibraryEvaluator evaluator = (PythonLibraryEvaluator) ctx.getStateObject(evaluatorId);
        if (evaluator == null) {
            evaluator = new PythonLibraryEvaluator(ctx.getJobletContext().getJobId(), evaluatorId, libMgr, pythonHome,
                    sitePkgs, pythonArgs, pythonEnv, router, ipcSys, ctx.getTaskAttemptId(), warningCollector,
                    sourceLoc);
            ctx.getJobletContext().registerDeallocatable(evaluator);
            evaluator.initialize();
            ctx.setStateObject(evaluator);
        }
        return evaluator;
    }
}
