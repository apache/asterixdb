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
package org.apache.hyracks.control.nc.io;

import java.util.concurrent.atomic.LongAdder;

import org.apache.hyracks.api.io.WorkspaceFileKind;

/**
 * Cumulative counts of the workspace files a node controller has created, and of the bytes written to and read from
 * them, by what they hold. Workspace files are where operators spill when their input does not fit in memory, so
 * these say how much a node spills and why.
 */
public final class WorkspaceFileStats {
    private static final WorkspaceFileKind[] KINDS = WorkspaceFileKind.values();

    private final LongAdder[] filesCreated = newAdders();
    private final LongAdder[] bytesWritten = newAdders();
    private final LongAdder[] bytesRead = newAdders();
    private final LongAdder[] jobsSpilled = newAdders();

    private static LongAdder[] newAdders() {
        LongAdder[] adders = new LongAdder[KINDS.length];
        for (int i = 0; i < adders.length; i++) {
            adders[i] = new LongAdder();
        }
        return adders;
    }

    void fileCreated(WorkspaceFileKind kind) {
        filesCreated[kind.ordinal()].increment();
    }

    void written(WorkspaceFileKind kind, long bytes) {
        if (bytes > 0) {
            bytesWritten[kind.ordinal()].add(bytes);
        }
    }

    void read(WorkspaceFileKind kind, long bytes) {
        if (bytes > 0) {
            bytesRead[kind.ordinal()].add(bytes);
        }
    }

    /**
     * Records that a job created workspace files on this node.
     *
     * @param kinds a mask with the bit {@code 1 << kind.ordinal()} set for each kind of file the job created
     */
    public void jobSpilled(int kinds) {
        for (int i = 0; i < KINDS.length; i++) {
            if ((kinds & (1 << i)) != 0) {
                jobsSpilled[i].increment();
            }
        }
    }

    public long getFilesCreated(WorkspaceFileKind kind) {
        return filesCreated[kind.ordinal()].sum();
    }

    public long getBytesWritten(WorkspaceFileKind kind) {
        return bytesWritten[kind.ordinal()].sum();
    }

    public long getBytesRead(WorkspaceFileKind kind) {
        return bytesRead[kind.ordinal()].sum();
    }

    /**
     * @return the number of jobs which created at least one workspace file of the given kind on this node
     */
    public long getJobsSpilled(WorkspaceFileKind kind) {
        return jobsSpilled[kind.ordinal()].sum();
    }

    /**
     * The workspace files present on a node at one moment, whoever created them.
     *
     * @param files the number of files
     * @param bytes their total size in bytes
     */
    public record Usage(long files, long bytes) {
    }
}
