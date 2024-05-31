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
package org.apache.asterix.column.metadata;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

/**
 * methods for defining the fieldName dictionary
 * which is used to encode a fieldName to an index.
 */
public interface IFieldNamesDictionary {
    /**
     * @return get all the inserted field names
     */
    List<IValueReference> getFieldNames();

    /**
     * @param fieldName fieldName byte array
     * @return returns index if field exist, otherwise insert fieldName and return the new index
     * @throws HyracksDataException
     */
    int getOrCreateFieldNameIndex(IValueReference fieldName) throws HyracksDataException;

    /**
     * @param fieldName fieldName string
     * @return returns index if field exist, otherwise insert fieldName and return the new index
     * @throws HyracksDataException
     */
    int getOrCreateFieldNameIndex(String fieldName) throws HyracksDataException;

    /**
     * @param fieldName
     * @return index of the field if exists otherwise -1
     * @throws HyracksDataException
     */
    int getFieldNameIndex(String fieldName) throws HyracksDataException;

    /**
     * @param index encoded index
     * @return the fieldName present at the requested index
     */
    IValueReference getFieldName(int index);

    /**
     * serialize the dictionary
     * @param output
     * @throws IOException
     */
    void serialize(DataOutput output) throws IOException;

    /**
     * resetting and rebuilding the dictionary
     * @param input
     * @throws IOException
     */
    void abort(DataInputStream input) throws IOException;

}
