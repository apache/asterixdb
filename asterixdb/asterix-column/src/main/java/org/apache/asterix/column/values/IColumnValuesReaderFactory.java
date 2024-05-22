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
package org.apache.asterix.column.values;

import java.io.DataInput;
import java.io.IOException;

import org.apache.asterix.column.metadata.PathInfoSerializer;
import org.apache.asterix.om.types.ATypeTag;

public interface IColumnValuesReaderFactory {
    /**
     * Create reader for a non-repeated primitive type
     *
     * @param typeTag     primitive type tag
     * @param columnIndex column index
     * @param maxLevel    maximum definition levels
     * @param primaryKey  is the value belongs to a primary key?
     * @return columnar reader
     */
    IColumnValuesReader createValueReader(ATypeTag typeTag, int columnIndex, int maxLevel, boolean primaryKey);

    /**
     * Create a reader for a repeated primitive type
     *
     * @param typeTag     primitive type tag
     * @param columnIndex column index
     * @param maxLevel    maximum definition levels
     * @param delimiters  the definition levels for array delimiters
     * @return columnar reader
     */
    IColumnValuesReader createValueReader(ATypeTag typeTag, int columnIndex, int maxLevel, int[] delimiters);

    /**
     * Create a reader from a serialized path info
     *
     * @param input column metadata info
     * @return columnar reader
     * @see PathInfoSerializer#writePathInfo(ATypeTag, int, boolean) how it serializes the path info
     */
    IColumnValuesReader createValueReader(DataInput input) throws IOException;
}
