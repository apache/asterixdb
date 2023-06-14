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

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

/**
 * Column writer for values
 */
public interface IColumnValuesWriter {

    /**
     * Reset the writer
     */
    void reset() throws HyracksDataException;

    /**
     * @return the corresponding index of a column
     */
    int getColumnIndex();

    /**
     * Write a value that are not MISSING or NULL
     *
     * @param tag   value type tag
     * @param value value reference
     */
    void writeValue(ATypeTag tag, IValueReference value) throws HyracksDataException;

    /**
     * Writing an anti-matter primary key value
     *
     * @param value value reference
     */
    void writeAntiMatter(ATypeTag tag, IValueReference value) throws HyracksDataException;

    /**
     * Write level
     *
     * @param level level of the value
     */
    void writeLevel(int level) throws HyracksDataException;

    /**
     * Convenient way to write a level multiple times
     *
     * @param level level of the value
     * @param count the number of level occurrences
     */
    void writeLevels(int level, int count) throws HyracksDataException;

    /**
     * For all writers except for {@link ATypeTag#NULL} writer, this method will return null
     *
     * @return the definition levels if this is a  {@link ATypeTag#NULL} writer, {@code null} otherwise
     */
    RunLengthIntArray getDefinitionLevelsIntArray();

    /**
     * Write NULL
     *
     * @param level at what level the NULL occurred
     */
    void writeNull(int level) throws HyracksDataException;

    /**
     * Write a non-unknown value from a reader. Not intended for writing {@link ATypeTag#NULL} or
     * {@link ATypeTag#MISSING}
     */
    void writeValue(IColumnValuesReader reader) throws HyracksDataException;

    /**
     * @return (probably) an overestimated size of the encoded values
     */
    int getEstimatedSize();

    /**
     * @param length the length of value to be return
     * @return (probably) an overestimated size needed to write a value with the given length
     */
    int getEstimatedSize(int length);

    /**
     * @return the allocated space in bytes
     */
    int getAllocatedSpace();

    /**
     * @return the total count of values
     */
    int getCount();

    /**
     * @return normalized minimum column value
     */
    long getNormalizedMinValue();

    /**
     * @return normalized maximum column value
     */
    long getNormalizedMaxValue();

    /**
     * Flush the columns value to output stream
     *
     * @param out output stream
     */
    void flush(OutputStream out) throws HyracksDataException;

    /**
     * Close the writer and release all allocated buffers
     */
    void close();

    /**
     * Serialize the writer
     *
     * @param output destination to which the writer should be serialized to
     */
    void serialize(DataOutput output) throws IOException;
}
