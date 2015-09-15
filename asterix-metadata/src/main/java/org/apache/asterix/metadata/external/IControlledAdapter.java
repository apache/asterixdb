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
package org.apache.asterix.metadata.external;

import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.INullWriterFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 *
 * @author alamouda
 *
 */
public interface IControlledAdapter extends Serializable {
    
    /**
     * 
     * @param ctx
     * @param recordDescriptors 
     * @throws Exception
     */
    public void initialize(IHyracksTaskContext ctx, INullWriterFactory iNullWriterFactory) throws Exception;

    /**
     * 
     * @param buffer
     * @param writer
     * @throws HyracksDataException
     */
    public void nextFrame(ByteBuffer buffer, IFrameWriter writer) throws Exception;

    /**
     * 
     * @param writer
     * @throws HyracksDataException
     */
    public void close(IFrameWriter writer) throws Exception;
    
    /**
     * Gives the adapter a chance to clean up
     * @param writer
     * @throws HyracksDataException
     */
    public void fail() throws Exception;
}