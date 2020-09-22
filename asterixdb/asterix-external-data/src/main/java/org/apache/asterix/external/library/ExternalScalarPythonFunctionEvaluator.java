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

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.WarningUtil;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.external.ipc.ExternalFunctionResultRouter;
import org.apache.asterix.external.ipc.PythonIPCProto;
import org.apache.asterix.external.library.msgpack.MessagePackerFromADM;
import org.apache.asterix.external.library.msgpack.MessageUnpackerToADM;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.resources.IDeallocatable;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.ipc.impl.IPCSystem;

class ExternalScalarPythonFunctionEvaluator extends ExternalScalarFunctionEvaluator {

    private final PythonLibraryEvaluator libraryEvaluator;

    private final ArrayBackedValueStorage resultBuffer = new ArrayBackedValueStorage();
    private final ByteBuffer argHolder;
    private final ByteBuffer outputWrapper;
    private final IEvaluatorContext evaluatorContext;
    private static final String ENTRYPOINT = "entrypoint.py";
    private static final String SITE_PACKAGES = "site-packages";

    private final IPointable[] argValues;

    ExternalScalarPythonFunctionEvaluator(IExternalFunctionInfo finfo, IScalarEvaluatorFactory[] args,
            IAType[] argTypes, IEvaluatorContext ctx, SourceLocation sourceLoc) throws HyracksDataException {
        super(finfo, args, argTypes, ctx);
        IApplicationConfig cfg = ctx.getServiceContext().getAppConfig();
        String pythonPathCmd = cfg.getString(NCConfig.Option.PYTHON_CMD);
        List<String> pythonArgs = new ArrayList<>();
        if (pythonPathCmd == null) {
            //if absolute path to interpreter is not specified, use environmental python
            pythonPathCmd = "/usr/bin/env";
            pythonArgs.add("python3");
        }
        File pythonPath = new File(pythonPathCmd);
        List<String> sitePkgs = new ArrayList<>();
        sitePkgs.add(SITE_PACKAGES);
        String[] addlSitePackages =
                ctx.getServiceContext().getAppConfig().getStringArray((NCConfig.Option.PYTHON_ADDITIONAL_PACKAGES));
        sitePkgs.addAll(Arrays.asList(addlSitePackages));
        if (cfg.getBoolean(NCConfig.Option.PYTHON_USE_BUNDLED_MSGPACK)) {
            sitePkgs.add("ipc" + File.separator + SITE_PACKAGES + File.separator);
        }
        String[] pythonArgsRaw = ctx.getServiceContext().getAppConfig().getStringArray(NCConfig.Option.PYTHON_ARGS);
        if (pythonArgsRaw != null) {
            pythonArgs.addAll(Arrays.asList(pythonArgsRaw));
        }
        StringBuilder sitePackagesPathBuilder = new StringBuilder();
        for (int i = 0; i < sitePkgs.size() - 1; i++) {
            sitePackagesPathBuilder.append(sitePkgs.get(i));
            sitePackagesPathBuilder.append(File.pathSeparator);
        }
        sitePackagesPathBuilder.append(sitePkgs.get(sitePkgs.size() - 1));

        try {
            libraryEvaluator = PythonLibraryEvaluator.getInstance(finfo, libraryManager, router, ipcSys, pythonPath,
                    ctx.getTaskContext(), sitePackagesPathBuilder.toString(), pythonArgs, ctx.getWarningCollector(),
                    sourceLoc);
        } catch (IOException | AsterixException e) {
            throw new HyracksDataException("Failed to initialize Python", e);
        }
        argValues = new IPointable[args.length];
        for (int i = 0; i < argValues.length; i++) {
            argValues[i] = VoidPointable.FACTORY.createPointable();
        }
        //TODO: these should be dynamic
        this.argHolder = ByteBuffer.wrap(new byte[Short.MAX_VALUE * 2]);
        this.outputWrapper = ByteBuffer.wrap(new byte[Short.MAX_VALUE * 2]);
        this.evaluatorContext = ctx;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        argHolder.clear();
        for (int i = 0, ln = argEvals.length; i < ln; i++) {
            argEvals[i].evaluate(tuple, argValues[i]);
            try {
                setArgument(i, argValues[i]);
            } catch (IOException e) {
                throw new HyracksDataException("Error evaluating Python UDF", e);
            }
        }
        try {
            ByteBuffer res = libraryEvaluator.callPython(argHolder, argTypes.length);
            resultBuffer.reset();
            wrap(res, resultBuffer.getDataOutput());
        } catch (Exception e) {
            throw new HyracksDataException("Error evaluating Python UDF", e);
        }
        result.set(resultBuffer.getByteArray(), resultBuffer.getStartOffset(), resultBuffer.getLength());
    }

    private static class PythonLibraryEvaluator extends AbstractStateObject implements IDeallocatable {
        Process p;
        IExternalFunctionInfo finfo;
        ILibraryManager libMgr;
        File pythonHome;
        PythonIPCProto proto;
        ExternalFunctionResultRouter router;
        IPCSystem ipcSys;
        String module;
        String clazz;
        String fn;
        String sitePkgs;
        List<String> pythonArgs;
        TaskAttemptId task;
        IWarningCollector warningCollector;
        SourceLocation sourceLoc;

        private PythonLibraryEvaluator(JobId jobId, PythonLibraryEvaluatorId evaluatorId, IExternalFunctionInfo finfo,
                ILibraryManager libMgr, File pythonHome, String sitePkgs, List<String> pythonArgs,
                ExternalFunctionResultRouter router, IPCSystem ipcSys, TaskAttemptId task,
                IWarningCollector warningCollector, SourceLocation sourceLoc) {
            super(jobId, evaluatorId);
            this.finfo = finfo;
            this.libMgr = libMgr;
            this.pythonHome = pythonHome;
            this.sitePkgs = sitePkgs;
            this.pythonArgs = pythonArgs;
            this.router = router;
            this.task = task;
            this.ipcSys = ipcSys;
            this.warningCollector = warningCollector;
            this.sourceLoc = sourceLoc;

        }

        public void initialize() throws IOException, AsterixException {
            PythonLibraryEvaluatorId fnId = (PythonLibraryEvaluatorId) id;
            List<String> externalIdents = finfo.getExternalIdentifier();
            PythonLibrary library = (PythonLibrary) libMgr.getLibrary(fnId.libraryDataverseName, fnId.libraryName);
            String wd = library.getFile().getAbsolutePath();
            String packageModule = externalIdents.get(0);
            String clazz;
            String fn;
            String externalIdent1 = externalIdents.get(1);
            int idx = externalIdent1.lastIndexOf('.');
            if (idx >= 0) {
                clazz = externalIdent1.substring(0, idx);
                fn = externalIdent1.substring(idx + 1);
            } else {
                clazz = "None";
                fn = externalIdent1;
            }
            this.fn = fn;
            this.clazz = clazz;
            this.module = packageModule;
            int port = ipcSys.getSocketAddress().getPort();
            List<String> args = new ArrayList<>();
            args.add(pythonHome.getAbsolutePath());
            args.addAll(pythonArgs);
            args.add(ENTRYPOINT);
            args.add(InetAddress.getLoopbackAddress().getHostAddress());
            args.add(Integer.toString(port));
            args.add(sitePkgs);
            ProcessBuilder pb = new ProcessBuilder(args.toArray(new String[0]));
            pb.directory(new File(wd));
            p = pb.start();
            proto = new PythonIPCProto(p.getOutputStream(), router, ipcSys);
            proto.start();
            proto.helo();
            proto.init(packageModule, clazz, fn);
        }

        ByteBuffer callPython(ByteBuffer arguments, int numArgs) throws Exception {
            ByteBuffer ret = null;
            try {
                ret = proto.call(arguments, numArgs);
            } catch (AsterixException e) {
                warningCollector
                        .warn(WarningUtil.forAsterix(sourceLoc, ErrorCode.EXTERNAL_UDF_EXCEPTION, e.getMessage()));
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
        }

        private static PythonLibraryEvaluator getInstance(IExternalFunctionInfo finfo, ILibraryManager libMgr,
                ExternalFunctionResultRouter router, IPCSystem ipcSys, File pythonHome, IHyracksTaskContext ctx,
                String sitePkgs, List<String> pythonArgs, IWarningCollector warningCollector, SourceLocation sourceLoc)
                throws IOException, AsterixException {
            PythonLibraryEvaluatorId evaluatorId =
                    new PythonLibraryEvaluatorId(finfo.getLibraryDataverseName(), finfo.getLibraryName());
            PythonLibraryEvaluator evaluator = (PythonLibraryEvaluator) ctx.getStateObject(evaluatorId);
            if (evaluator == null) {
                evaluator = new PythonLibraryEvaluator(ctx.getJobletContext().getJobId(), evaluatorId, finfo, libMgr,
                        pythonHome, sitePkgs, pythonArgs, router, ipcSys, ctx.getTaskAttemptId(), warningCollector,
                        sourceLoc);
                ctx.registerDeallocatable(evaluator);
                evaluator.initialize();
                ctx.setStateObject(evaluator);
            }
            return evaluator;
        }
    }

    private static final class PythonLibraryEvaluatorId {

        private final DataverseName libraryDataverseName;

        private final String libraryName;

        private PythonLibraryEvaluatorId(DataverseName libraryDataverseName, String libraryName) {
            this.libraryDataverseName = Objects.requireNonNull(libraryDataverseName);
            this.libraryName = Objects.requireNonNull(libraryName);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            PythonLibraryEvaluatorId that = (PythonLibraryEvaluatorId) o;
            return libraryDataverseName.equals(that.libraryDataverseName) && libraryName.equals(that.libraryName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(libraryDataverseName, libraryName);
        }
    }

    private void setArgument(int index, IValueReference valueReference) throws IOException {
        IAType type = argTypes[index];
        ATypeTag tag = type.getTypeTag();
        switch (tag) {
            case ANY:
                TaggedValuePointable pointy = TaggedValuePointable.FACTORY.createPointable();
                pointy.set(valueReference);
                ATypeTag rtTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(pointy.getTag());
                IAType rtType = TypeTagUtil.getBuiltinTypeByTag(rtTypeTag);
                MessagePackerFromADM.pack(valueReference, rtType, argHolder);
                break;
            default:
                MessagePackerFromADM.pack(valueReference, type, argHolder);
                break;
        }
    }

    private void wrap(ByteBuffer resultWrapper, DataOutput out) throws HyracksDataException {
        //TODO: output wrapper needs to grow with result wrapper
        outputWrapper.clear();
        outputWrapper.position(0);
        MessageUnpackerToADM.unpack(resultWrapper, outputWrapper, true);
        try {
            out.write(outputWrapper.array(), 0, outputWrapper.position() + outputWrapper.arrayOffset());
        } catch (IOException e) {
            throw new HyracksDataException(e.getMessage());
        }

    }
}
