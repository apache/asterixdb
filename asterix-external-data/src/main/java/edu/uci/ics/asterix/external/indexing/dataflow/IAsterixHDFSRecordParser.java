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

import java.io.DataOutput;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import edu.uci.ics.asterix.om.types.ARecordType;

/**
 * This interface is provided for users to implements in order to support their own
 * it should be included sometimes in the future in the external library
 * input parsing
 * @author alamouda
 *
 */
public interface IAsterixHDFSRecordParser {

    /**
     * This method is called once upon creating the serde before starting to parse objects
     * @param record
     *  The description of the expected dataset record.
     * @param arguments
     *  The arguments passed when creating the external dataset
     */
    public void initialize(ARecordType record, Map<String, String> arguments, Configuration hadoopConfig) throws Exception;
    
    /**
     * This function takes an object, parse it and then serialize it into an adm record in the output buffer
     * @param object
     *  the serialized I/O object
     * @param output
     *  output buffer where deserialized object need to be serialized
     */
    public void parse(Object object, DataOutput output) throws Exception;

}
