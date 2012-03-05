/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.external.data.parser;

import java.util.Map;

import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * Represents an parser that processes an input stream to form records of a
 * given type. The parser creates frames that are flushed using a frame writer.
 */
public interface IDataParser {

    /**
     * @param atype
     *            The record type associated with each record output by the
     *            parser
     * @param configuration
     *            Any configuration parameters for the parser
     */
    public void configure(Map<String, String> configuration);

    /**
     * Initializes the instance. An implementation may use the passed-in
     * configuration parameters, the output record type to initialize itself so
     * that it can parse an input stream to form records of the given type.
     * 
     * @param configuration
     *            Any configuration parameters for the parser
     * @param ctx
     *            The runtime HyracksStageletContext.
     */
    public void initialize(ARecordType recordType, IHyracksTaskContext ctx);

    /**
     * Parses the input stream to produce records of the configured type and
     * uses the frame writer instance to flush frames containing the produced
     * records.
     * 
     * @param in
     *            The source input stream
     * @param frameWriter
     *            A frame writer instance that is used for flushing frames to
     *            the recipient operator
     * @throws HyracksDataException
     */
    public void parse(IFrameWriter frameWriter) throws HyracksDataException;

}
