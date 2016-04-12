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
package org.apache.hyracks.algebricks.data.impl;

import java.io.DataOutput;

import org.apache.hyracks.api.dataflow.value.INullWriter;
import org.apache.hyracks.api.dataflow.value.INullWriterFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class NoopNullWriterFactory implements INullWriterFactory {

    private static final long serialVersionUID = 1L;
    public static final NoopNullWriterFactory INSTANCE = new NoopNullWriterFactory();

    private NoopNullWriterFactory() {
    }

    @Override
    public INullWriter createNullWriter() {

        return new INullWriter() {

            @Override
            public void writeNull(DataOutput out) throws HyracksDataException {
                // do nothing
            }
        };
    }

}
