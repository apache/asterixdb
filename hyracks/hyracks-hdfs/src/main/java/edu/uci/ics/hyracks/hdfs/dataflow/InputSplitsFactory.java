/*
 * Copyright 2009-2012 by The Regents of the University of California
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

package edu.uci.ics.hyracks.hdfs.dataflow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.mapred.InputSplit;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

@SuppressWarnings({ "deprecation", "rawtypes" })
public class InputSplitsFactory implements Serializable {

    private static final long serialVersionUID = 1L;
    private byte[] splitBytes;
    private String splitClassName;

    public InputSplitsFactory(InputSplit[] splits) throws HyracksDataException {
        splitBytes = splitsToBytes(splits);
        if (splits.length > 0) {
            splitClassName = splits[0].getClass().getName();
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
            throw new HyracksDataException(e);
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
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            DataInputStream dis = new DataInputStream(bis);
            int size = dis.readInt();
            InputSplit[] splits = new InputSplit[size];
            for (int i = 0; i < size; i++) {
                Class splitNameClass = Class.forName(splitClassName);
                splits[i] = (InputSplit) splitNameClass.newInstance();
                splits[i].readFields(dis);
            }
            dis.close();
            return splits;
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }
}
