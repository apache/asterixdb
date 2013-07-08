/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hyracks.hdfs2.dataflow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Serializable;

import org.apache.hadoop.mapreduce.Job;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ConfFactory implements Serializable {
    private static final long serialVersionUID = 1L;
    private byte[] confBytes;

    public ConfFactory(Job conf) throws HyracksDataException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            conf.getConfiguration().write(dos);
            confBytes = bos.toByteArray();
            dos.close();
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    public Job getConf() throws HyracksDataException {
        try {
            Job conf = new Job();
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(confBytes));
            conf.getConfiguration().readFields(dis);
            dis.close();
            return conf;
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }
}
