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
package org.apache.asterix.om.values;

import org.apache.asterix.om.lazy.metadata.stream.in.AbstractRowBytesInputStream;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

public interface IRowValuesReader extends Comparable<IRowValuesReader> {
    /**
     * Reset the reader
     *
     * @param in         input stream that contains the values
     * @param tupleCount tuple count this column batch belongs to
     */
    void reset(AbstractRowBytesInputStream in, int tupleCount) throws HyracksDataException;

    /* ***********************
     * Iteration functions
     * ***********************
     */

    /**
     * Move the next value
     *
     * @return true if next value was found, false if the end of the values
     */
    boolean next() throws HyracksDataException;

    /* ***********************
     * Information functions
     * ***********************
     */
    ATypeTag getTypeTag();

    /**
     * @return columnIndex
     */
    int getColumnIndex();

    /**
     * @return Level of the value, which determines if it is NULL, MISSING, or VALUE
     */
    int getLevel();

    /**
     * @return is the current value MISSING
     */
    boolean isMissing();

    /**
     * @return is the current value NULL
     */
    boolean isNull();

    /**
     * @return is an actual value (i.e., neither NULL or MISSING)
     */
    boolean isValue();

    /**
     * @return is this column belongs to an array or multiset
     */
    boolean isRepeated();

    /**
     * @return is it an end of an array (arrays could be nested, and we can hit different delimiters)
     */
    boolean isDelimiter();

    /**
     * @return is the last delimiter (the end of all nested arrays)
     */
    boolean isLastDelimiter();

    boolean isRepeatedValue();

    int getNumberOfDelimiters();

    /**
     * @return which delimiter was returned (nested arrays have different delimiter indexes)
     */
    int getDelimiterIndex();

    /* ***********************
     * Value functions
     * ***********************
     */

    long getLong();

    double getDouble();

    boolean getBoolean();

    IValueReference getBytes();

    /* ***********************
     * Write functions
     * ***********************
     */

    /**
     * Write the content of reader to the writer
     *
     * @param writer   to which is the content of this reader is written to
     * @param callNext should call next on write
     */
    void write(IRowValuesWriter writer, boolean callNext) throws HyracksDataException;

    /**
     * Write the content of reader to the writer
     *
     * @param writer to which is the content of this reader is written to
     * @param count  number of values to write
     */
    void write(IRowValuesWriter writer, int count) throws HyracksDataException;

    /**
     * Skip values
     *
     * @param count the number of values should be skipped
     */
    void skip(int count) throws HyracksDataException;
}
