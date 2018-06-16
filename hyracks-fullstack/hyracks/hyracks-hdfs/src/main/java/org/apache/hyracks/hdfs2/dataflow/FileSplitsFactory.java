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

package org.apache.hyracks.hdfs2.dataflow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hyracks.api.exceptions.HyracksDataException;

@SuppressWarnings("rawtypes")
public class FileSplitsFactory implements Serializable {

    private static final long serialVersionUID = 1L;
    private byte[] splitBytes;
    private String splitClassName;

    public FileSplitsFactory(List<FileSplit> splits) throws HyracksDataException {
        splitBytes = splitsToBytes(splits);
        if (splits.size() > 0) {
            splitClassName = splits.get(0).getClass().getName();
        }
    }

    public List<FileSplit> getSplits() throws HyracksDataException {
        return bytesToSplits(splitBytes);
    }

    /**
     * Convert splits to bytes.
     *
     * @param splits
     *            input splits
     * @return bytes which serialize the splits
     * @throws IOException
     */
    private byte[] splitsToBytes(List<FileSplit> splits) throws HyracksDataException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            dos.writeInt(splits.size());
            int size = splits.size();
            for (int i = 0; i < size; i++) {
                splits.get(i).write(dos);
            }
            dos.close();
            return bos.toByteArray();
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Covert bytes to splits.
     *
     * @param bytes
     * @return
     * @throws HyracksDataException
     */
    private List<FileSplit> bytesToSplits(byte[] bytes) throws HyracksDataException {
        try {
            Class splitClass = Class.forName(splitClassName);
            Constructor[] constructors = splitClass.getDeclaredConstructors();
            Constructor defaultConstructor = null;
            for (Constructor constructor : constructors) {
                if (constructor.getParameterTypes().length == 0) {
                    constructor.setAccessible(true);
                    defaultConstructor = constructor;
                }
            }
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            DataInputStream dis = new DataInputStream(bis);
            int size = dis.readInt();
            List<FileSplit> splits = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                splits.add((FileSplit) defaultConstructor.newInstance());
                splits.get(i).readFields(dis);
            }
            dis.close();
            return splits;
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }
}
