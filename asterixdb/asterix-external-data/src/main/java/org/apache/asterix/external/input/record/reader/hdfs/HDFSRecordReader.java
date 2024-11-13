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
package org.apache.asterix.external.input.record.reader.hdfs;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class HDFSRecordReader<K, V extends Writable> extends AbstractHDFSRecordReader<K, V> {

    public HDFSRecordReader(boolean[] read, InputSplit[] inputSplits, String[] readSchedule, String nodeName,
            JobConf conf, UserGroupInformation ugi) {
        super(read, inputSplits, readSchedule, nodeName, conf, ugi);
    }

    @Override
    protected RecordReader<K, V> getRecordReader(int splitIndex) throws IOException {
        try {
            reader = ugi == null ? getReader(splitIndex)
                    : ugi.doAs((PrivilegedExceptionAction<RecordReader<K, V>>) () -> getReader(splitIndex));
        } catch (InterruptedException ex) {
            throw HyracksDataException.create(ex);
        }
        if (key == null) {
            key = reader.createKey();
            value = reader.createValue();
        }
        return reader;
    }

    @SuppressWarnings("unchecked")
    private RecordReader<K, V> getReader(int splitIndex) throws IOException {
        return (RecordReader<K, V>) inputFormat.getRecordReader(inputSplits[splitIndex], conf, Reporter.NULL);
    }

    @Override
    protected boolean onNextInputSplit() {
        return false;
    }

    @Override
    public boolean stop() {
        return false;
    }

    @Override
    public RecordReader<K, V> getReader() {
        return reader;
    }
}
