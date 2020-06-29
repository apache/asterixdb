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

package org.apache.asterix.external.operators;

import java.io.IOException;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class LibraryDeployAbortOperatorDescriptor extends AbstractLibraryOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LogManager.getLogger(LibraryDeployAbortOperatorDescriptor.class);

    public LibraryDeployAbortOperatorDescriptor(IOperatorDescriptorRegistry spec, DataverseName dataverseName,
            String libraryName) {
        super(spec, dataverseName, libraryName);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new AbstractLibraryNodePushable(ctx) {
            @Override
            protected void execute() throws IOException {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Abort deployment of library {}.{}", dataverseName, libraryName);
                }

                FileReference libDir = getLibraryDir();

                FileReference rev0 = getRev0Dir();
                FileReference rev1 = getRev1Dir();
                if (rev0.getFile().exists() || rev1.getFile().exists()) {
                    // #. drop 'stage' dir.
                    FileReference stage = getStageDir();
                    dropIfExists(stage);
                    flushDirectory(libDir);
                } else {
                    // #. drop the whole library dir
                    dropIfExists(libDir);
                    flushDirectory(libDir.getFile().getParentFile());
                }
            }
        };
    }
}