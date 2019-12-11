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
package org.apache.hyracks.api.io;

import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IWorkspaceFileFactory {

    /**
     * Creates an unmanaged file. Unmanaged files are not automatically deleted. The caller has to delete them.
     *
     * @param prefix a meaningful string to start the file name with.
     */
    public FileReference createUnmanagedWorkspaceFile(String prefix) throws HyracksDataException;

    /**
     * Creates a managed file. Managed files are automatically deleted at a certain point. This depends on which
     * context the file is created in. Files created in the {@link IHyracksJobletContext job context} will be deleted
     * as soon as the job is done. Files created in the {@link IHyracksTaskContext task context} will be deleted as
     * soon as the task is done.
     *
     * @param prefix a meaningful string to start the file name with.
     */
    public FileReference createManagedWorkspaceFile(String prefix) throws HyracksDataException;
}
