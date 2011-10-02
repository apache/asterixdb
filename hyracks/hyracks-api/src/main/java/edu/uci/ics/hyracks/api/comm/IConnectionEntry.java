/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.api.comm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.UUID;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IConnectionEntry {
    ByteBuffer getReadBuffer();

    SelectionKey getSelectionKey();

    void setDataReceiveListener(IDataReceiveListener listener);

    void attach(Object attachment);

    Object getAttachment();

    void close() throws IOException;

    void write(ByteBuffer buffer) throws HyracksDataException;

    UUID getJobId();

    UUID getStageId();

    void setJobId(UUID jobId);

    void setStageId(UUID stageId);

    boolean aborted();

    void abort();
}