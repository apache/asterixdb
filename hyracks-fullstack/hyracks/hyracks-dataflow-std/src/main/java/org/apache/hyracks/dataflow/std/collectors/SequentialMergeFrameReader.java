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
package org.apache.hyracks.dataflow.std.collectors;

import java.util.LinkedList;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameReader;
import org.apache.hyracks.api.exceptions.HyracksDataException;

// TODO(ali): consider sort-concat-merge as an alternative.
public class SequentialMergeFrameReader implements IFrameReader {
    private final int numSenders;
    private final IPartitionBatchManager partitionBatchManager;
    private final LinkedList<IFrameReader> senders;
    private boolean isOpen;

    public SequentialMergeFrameReader(int numSenders, IPartitionBatchManager partitionBatchManager) {
        this.numSenders = numSenders;
        this.partitionBatchManager = partitionBatchManager;
        this.senders = new LinkedList<>();
        this.isOpen = false;
    }

    @Override
    public void open() throws HyracksDataException {
        if (!isOpen) {
            isOpen = true;
            // get all the senders and open them one by one
            partitionBatchManager.getNextBatch(senders, numSenders);
            for (IFrameReader sender : senders) {
                sender.open();
            }
        }
    }

    @Override
    public boolean nextFrame(IFrame outFrame) throws HyracksDataException {
        IFrameReader currentSender;
        while (!senders.isEmpty()) {
            // process the sender at the beginning of the sequence
            currentSender = senders.getFirst();
            outFrame.reset();
            if (currentSender.nextFrame(outFrame)) {
                return true;
            } else {
                // done with the current sender, close it, remove it from the Q and process the next one in sequence
                currentSender.close();
                senders.removeFirst();
            }
        }
        // done with all senders
        return false;
    }

    @Override
    public void close() throws HyracksDataException {
        for (IFrameReader sender : senders) {
            sender.close();
        }
    }
}
