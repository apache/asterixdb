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
package org.apache.asterix.external.indexing.input;

import java.io.InputStream;

import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;

/***
 * an abstract class to be used for reading hdfs based datasets one record at a time <- used for indexing->
 */
@SuppressWarnings("deprecation")
public abstract class AbstractHDFSReader extends InputStream {

    /***
     * This function should be called once to do initial setup before starting to read records
     * 
     * @return true if ready for reading
     */
    abstract public boolean initialize() throws Exception;

    /***
     * @return the next object read or null if reached end of stream
     */
    abstract public Object readNext() throws Exception;

    /**
     * @return the file name of the current filesplit being read
     * @throws Exception
     *             in case of end of records is reached
     */
    abstract public String getFileName() throws Exception;

    /**
     * @return return the reader position of last record read
     * @throws Exception
     *             in case of end of records is reached
     */
    abstract public long getReaderPosition() throws Exception;

    /**
     * @return the file number of the file being read
     * @throws Exception
     */
    abstract public int getFileNumber() throws Exception;

    protected Reporter getReporter() {
        Reporter reporter = new Reporter() {

            @Override
            public Counter getCounter(Enum<?> arg0) {
                return null;
            }

            @Override
            public Counter getCounter(String arg0, String arg1) {
                return null;
            }

            @Override
            public InputSplit getInputSplit() throws UnsupportedOperationException {
                return null;
            }

            @Override
            public void incrCounter(Enum<?> arg0, long arg1) {
            }

            @Override
            public void incrCounter(String arg0, String arg1, long arg2) {
            }

            @Override
            public void setStatus(String arg0) {
            }

            @Override
            public void progress() {
            }

            public float getProgress() {
                return 0.0f;
            }
        };

        return reporter;
    }

}
