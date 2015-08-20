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
/*
 * Copyright 2009-2010 University of California, Irvine
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
package edu.uci.ics.hyracks.dataflow.hadoop.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

public class InputSplitsProxy implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Class[] isClasses;
    private final byte[] bytes;

    public InputSplitsProxy(JobConf conf, Object[] inputSplits) throws IOException {
        isClasses = new Class[inputSplits.length];
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        if (conf.getUseNewMapper()) {
            for (int i = 0; i < inputSplits.length; ++i) {
                isClasses[i] = ((org.apache.hadoop.mapreduce.InputSplit) inputSplits[i]).getClass();
                ((Writable) inputSplits[i]).write(dos);
            }
        } else {
            for (int i = 0; i < inputSplits.length; ++i) {
                isClasses[i] = ((org.apache.hadoop.mapred.InputSplit) inputSplits[i]).getClass();
                ((Writable) inputSplits[i]).write(dos);
            }
        }
        dos.close();
        bytes = baos.toByteArray();

    }

    public Object[] toInputSplits(JobConf jobConf) throws InstantiationException, IllegalAccessException, IOException {
        Object[] splits = new Object[isClasses.length];
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
        for (int i = 0; i < splits.length; ++i) {
            splits[i] = ReflectionUtils.newInstance(isClasses[i], jobConf);
            if (jobConf.getUseNewMapper()) {
                ((Writable) splits[i]).readFields(dis);
            } else {
                ((Writable) splits[i]).readFields(dis);
            }
        }
        return splits;
    }
}