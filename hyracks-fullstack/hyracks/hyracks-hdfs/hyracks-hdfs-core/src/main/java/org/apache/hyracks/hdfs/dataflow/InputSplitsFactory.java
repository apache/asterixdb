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

package org.apache.hyracks.hdfs.dataflow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hyracks.api.exceptions.HyracksDataException;

@SuppressWarnings({ "rawtypes" })
public class InputSplitsFactory implements Serializable {

    private static final long serialVersionUID = 1L;
    private byte[] splitBytes;
    private String splitClassName;

    public InputSplitsFactory(InputSplit[] splits) throws HyracksDataException {
        splitBytes = splitsToBytes(splits);
        if (splits.length > 0) {
            splitClassName = splits[0].getClass().getName();
        } else {
            splitClassName = FileSplit.class.getName();
        }
    }

    public InputSplit[] getSplits() throws HyracksDataException {
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
    private byte[] splitsToBytes(InputSplit[] splits) throws HyracksDataException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            dos.writeInt(splits.length);
            for (int i = 0; i < splits.length; i++) {
                splits[i].write(dos);
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
    private InputSplit[] bytesToSplits(byte[] bytes) throws HyracksDataException {
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
            InputSplit[] splits = new InputSplit[size];
            for (int i = 0; i < size; i++) {
                splits[i] = (InputSplit) defaultConstructor.newInstance();
                splits[i].readFields(dis);
            }
            dis.close();
            return splits;
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }
}
