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

package org.apache.hyracks.tests.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.nc.resources.memory.FrameManager;
import org.apache.hyracks.dataflow.common.comm.io.FrameFixedFieldAppender;

public class InputFrameGenerator {

    protected final FrameManager manager;

    public InputFrameGenerator(int initialFrameSize) {
        manager = new FrameManager(initialFrameSize);
    }

    public List<IFrame> generateDataFrame(RecordDescriptor recordDescriptor, List<Object[]> listOfObject)
            throws HyracksDataException {
        List<IFrame> listFrame = new ArrayList<>();
        VSizeFrame frame = new VSizeFrame(manager);
        FrameFixedFieldAppender appender = new FrameFixedFieldAppender(recordDescriptor.getFieldCount());
        appender.reset(frame, true);

        ByteArrayOutputStream baos = new ByteArrayOutputStream(manager.getInitialFrameSize());
        DataOutputStream ds = new DataOutputStream(baos);
        for (Object[] objs : listOfObject) {
            for (int i = 0; i < recordDescriptor.getFieldCount(); i++) {
                baos.reset();
                recordDescriptor.getFields()[i].serialize(objs[i], ds);
                if (!appender.appendField(baos.toByteArray(), 0, baos.size())) {
                    listFrame.add(frame);
                    frame = new VSizeFrame(manager);
                    appender.resetWithLeftOverData(frame);
                    if (!appender.appendField(baos.toByteArray(), 0, baos.size())) {
                        throw new HyracksDataException("Should never happen!");
                    }
                }
            }
        }
        listFrame.add(frame);
        return listFrame;
    }

}
