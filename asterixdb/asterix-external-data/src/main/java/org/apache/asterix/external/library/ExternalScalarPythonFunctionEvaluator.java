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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.WarningUtil;
import org.apache.asterix.common.functions.FunctionSignature;
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

    private final IPointable[] argValues;

    ExternalScalarPythonFunctionEvaluator(IExternalFunctionInfo finfo, IScalarEvaluatorFactory[] args,
            IAType[] argTypes, IEvaluatorContext ctx, SourceLocation sourceLoc) throws HyracksDataException {
        super(finfo, args, argTypes, ctx);

        File pythonPath = new File(ctx.getServiceContext().getAppConfig().getString(NCConfig.Option.PYTHON_HOME));
        DataverseName dataverseName = FunctionSignature.getDataverseName(finfo.getFunctionIdentifier());
        try {
            libraryEvaluator = PythonLibraryEvaluator.getInstance(dataverseName, finfo, libraryManager, router, ipcSys,
                    pythonPath, ctx.getTaskContext(), ctx.getWarningCollector(), sourceLoc);
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
        TaskAttemptId task;
        IWarningCollector warningCollector;
        SourceLocation sourceLoc;

        private PythonLibraryEvaluator(JobId jobId, PythonLibraryEvaluatorId evaluatorId, IExternalFunctionInfo finfo,
                ILibraryManager libMgr, File pythonHome, ExternalFunctionResultRouter router, IPCSystem ipcSys,
                TaskAttemptId task, IWarningCollector warningCollector, SourceLocation sourceLoc) {
            super(jobId, evaluatorId);
            this.finfo = finfo;
            this.libMgr = libMgr;
            this.pythonHome = pythonHome;
            this.router = router;
            this.task = task;
            this.ipcSys = ipcSys;
            this.warningCollector = warningCollector;
            this.sourceLoc = sourceLoc;

        }

        public void initialize() throws IOException, AsterixException {
            PythonLibraryEvaluatorId fnId = (PythonLibraryEvaluatorId) id;
            List<String> externalIdents = finfo.getExternalIdentifier();
            PythonLibrary library = (PythonLibrary) libMgr.getLibrary(fnId.dataverseName, fnId.libraryName);
            String wd = library.getFile().getAbsolutePath();
            String packageModule = externalIdents.get(0);
            String clazz = "None";
            String fn;
            if (externalIdents.size() > 2) {
                clazz = externalIdents.get(1);
                fn = externalIdents.get(2);
            } else {
                fn = externalIdents.get(1);
            }
            this.fn = fn;
            this.clazz = clazz;
            this.module = packageModule;
            int port = ipcSys.getSocketAddress().getPort();
            ProcessBuilder pb = new ProcessBuilder(pythonHome.getAbsolutePath(), ENTRYPOINT,
                    InetAddress.getLoopbackAddress().getHostAddress(), Integer.toString(port));
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

        private static PythonLibraryEvaluator getInstance(DataverseName dataverseName, IExternalFunctionInfo finfo,
                ILibraryManager libMgr, ExternalFunctionResultRouter router, IPCSystem ipcSys, File pythonHome,
                IHyracksTaskContext ctx, IWarningCollector warningCollector, SourceLocation sourceLoc)
                throws IOException, AsterixException {
            PythonLibraryEvaluatorId evaluatorId = new PythonLibraryEvaluatorId(dataverseName, finfo.getLibrary());
            PythonLibraryEvaluator evaluator = (PythonLibraryEvaluator) ctx.getStateObject(evaluatorId);
            if (evaluator == null) {
                evaluator = new PythonLibraryEvaluator(ctx.getJobletContext().getJobId(), evaluatorId, finfo, libMgr,
                        pythonHome, router, ipcSys, ctx.getTaskAttemptId(), warningCollector, sourceLoc);
                ctx.registerDeallocatable(evaluator);
                evaluator.initialize();
                ctx.setStateObject(evaluator);
            }
            return evaluator;
        }
    }

    private static final class PythonLibraryEvaluatorId {

        private final DataverseName dataverseName;

        private final String libraryName;

        private PythonLibraryEvaluatorId(DataverseName dataverseName, String libraryName) {
            this.dataverseName = Objects.requireNonNull(dataverseName);
            this.libraryName = Objects.requireNonNull(libraryName);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            PythonLibraryEvaluatorId that = (PythonLibraryEvaluatorId) o;
            return dataverseName.equals(that.dataverseName) && libraryName.equals(that.libraryName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataverseName, libraryName);
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
