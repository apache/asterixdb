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
package edu.uci.ics.asterix.metadata.external;

import java.io.Serializable;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

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