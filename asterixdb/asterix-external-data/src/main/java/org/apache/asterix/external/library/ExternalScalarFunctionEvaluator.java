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

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.external.ipc.ExternalFunctionResultRouter;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.ipc.impl.IPCSystem;

public abstract class ExternalScalarFunctionEvaluator implements IScalarEvaluator {

    protected final IExternalFunctionInfo finfo;
    protected final IScalarEvaluator[] argEvals;
    protected final IAType[] argTypes;
    protected final ILibraryManager libraryManager;
    protected final ExternalFunctionResultRouter router;
    protected final IPCSystem ipcSys;

    public ExternalScalarFunctionEvaluator(IExternalFunctionInfo finfo, IScalarEvaluatorFactory[] args,
            IAType[] argTypes, IEvaluatorContext context) throws HyracksDataException {
        this.finfo = finfo;
        this.argTypes = argTypes;
        argEvals = new IScalarEvaluator[args.length];
        for (int i = 0; i < args.length; i++) {
            argEvals[i] = args[i].createScalarEvaluator(context);
        }
        libraryManager =
                ((INcApplicationContext) context.getServiceContext().getApplicationContext()).getLibraryManager();
        router = libraryManager.getRouter();
        ipcSys = libraryManager.getIPCI();
    }
}
