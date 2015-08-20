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
package edu.uci.ics.hyracks.algebricks.data;

import java.io.Serializable;

import edu.uci.ics.hyracks.api.dataflow.value.IResultSerializerFactory;

public interface IResultSerializerFactoryProvider extends Serializable {
    /**
     * Returns a result serializer factory
     * 
     * @param fields
     *            - A position of the fields in the order it should be written in the output.
     * @param printerFactories
     *            - A printer factory array to print the tuple containing different fields.
     * @param writerFactory
     *            - A writer factory to write the serialized data to the print stream.
     * @param inputRecordDesc
     *            - The record descriptor describing the input frame to be serialized.
     * @return A new instance of result serialized appender.
     */
    public IResultSerializerFactory getAqlResultSerializerFactoryProvider(int[] fields,
            IPrinterFactory[] printerFactories, IAWriterFactory writerFactory);
}
