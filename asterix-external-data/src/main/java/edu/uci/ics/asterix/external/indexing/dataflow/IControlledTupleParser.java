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
package edu.uci.ics.asterix.external.indexing.dataflow;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * This interface is to be implemented by parsers used in a pipelined hyracks job where input is not ready all at once
 */
public interface IControlledTupleParser {
    /**
     * This function should flush the tuples setting in the frame writer buffer
     * and free all resources
     */
    public void close(IFrameWriter writer) throws Exception;

    /**
     * This function is called when there are more data ready for parsing in the input stream
     * @param writer
     *          a frame writer that is used to push outgoig frames 
     * @param frameBuffer 
     *          a frame buffer containing the incoming tuples, used for propagating fields.
     */
    public void parseNext(IFrameWriter writer, ByteBuffer frameBuffer) throws HyracksDataException;
}
