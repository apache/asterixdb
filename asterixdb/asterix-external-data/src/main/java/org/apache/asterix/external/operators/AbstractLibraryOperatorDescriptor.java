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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.external.library.ExternalLibraryManager;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

abstract class AbstractLibraryOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    protected final DataverseName dataverseName;

    protected final String libraryName;

    public AbstractLibraryOperatorDescriptor(IOperatorDescriptorRegistry spec, DataverseName dataverseName,
            String libraryName) {
        super(spec, 0, 0);
        this.dataverseName = dataverseName;
        this.libraryName = libraryName;
    }

    protected abstract class AbstractLibraryNodePushable extends AbstractOperatorNodePushable {

        protected final IHyracksTaskContext ctx;

        protected IIOManager ioManager;

        protected ILibraryManager libraryManager;

        private FileReference libraryDir;

        protected AbstractLibraryNodePushable(IHyracksTaskContext ctx) {
            this.ctx = ctx;
        }

        protected abstract void execute() throws IOException;

        @Override
        public final void initialize() throws HyracksDataException {
            INcApplicationContext runtimeCtx =
                    (INcApplicationContext) ctx.getJobletContext().getServiceContext().getApplicationContext();
            ioManager = runtimeCtx.getIoManager();
            libraryManager = runtimeCtx.getLibraryManager();
            libraryDir = libraryManager.getLibraryDir(dataverseName, libraryName);
            try {
                execute();
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }

        protected FileReference getLibraryDir() {
            return libraryDir;
        }

        protected FileReference getRev0Dir() {
            return libraryDir.getChild(ExternalLibraryManager.REV_0_DIR_NAME);
        }

        protected FileReference getRev1Dir() {
            return libraryDir.getChild(ExternalLibraryManager.REV_1_DIR_NAME);
        }

        protected FileReference getStageDir() {
            return libraryDir.getChild(ExternalLibraryManager.STAGE_DIR_NAME);
        }

        // does not flush any directories
        protected void dropIfExists(FileReference fileRef) throws HyracksDataException {
            if (fileRef.getFile().exists()) {
                libraryManager.dropLibraryPath(fileRef);
            }
        }

        protected void move(FileReference src, FileReference dest) throws IOException {
            dropIfExists(dest);
            Files.move(src.getFile().toPath(), dest.getFile().toPath(), StandardCopyOption.ATOMIC_MOVE);
        }

        protected void mkdir(FileReference dir) throws IOException {
            Files.createDirectory(dir.getFile().toPath());
        }

        protected void flushDirectory(FileReference dir) throws IOException {
            flushDirectory(dir.getFile());
        }

        protected void flushDirectory(File dir) throws IOException {
            IoUtil.flushDirectory(dir);
        }

        protected void flushDirectory(Path dir) throws IOException {
            IoUtil.flushDirectory(dir);
        }

        protected void closeLibrary() throws HyracksDataException {
            libraryManager.closeLibrary(dataverseName, libraryName);
        }

        @Override
        public final void deinitialize() {
        }

        @Override
        public final int getInputArity() {
            return 0;
        }

        @Override
        public final void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
        }

        @Override
        public IFrameWriter getInputFrameWriter(int index) {
            return null;
        }
    }
}