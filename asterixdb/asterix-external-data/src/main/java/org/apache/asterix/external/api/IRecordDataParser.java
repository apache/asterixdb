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
package org.apache.asterix.external.api;

import java.io.DataOutput;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IRecordDataParser<T> extends IDataParser {

    /**
     * Parses the input record and writes the result into the {@code out}. Implementations should only write to the
     * {@code out} if the record is parsed successfully. If parsing fails, the {@code out} should never be touched. In
     * other words, no partial writing in case of failure. Additionally, implementations may choose to issue a
     * warning and/or throw an exception in case of failure.
     *
     * @param record input record to parse
     * @param out output where the parsed record is written into
     *
     * @return true if the record was parsed successfully and written to out. False, otherwise.
     * @throws HyracksDataException HyracksDataException
     */
    public boolean parse(IRawRecord<? extends T> record, DataOutput out) throws HyracksDataException;

    /**
     * Configures the parser with information suppliers from the {@link IRecordReader} data source.
     *
     * @param dataSourceName data source name supplier
     * @param lineNumber line number supplier
     */
    default void configure(Supplier<String> dataSourceName, LongSupplier lineNumber) {
    }
}
