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

package org.apache.asterix.builders;

import java.io.DataOutput;

import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

/**
 * A record builder helps to construct Asterix Records in a serialized format.
 * The intended use is to add a few fields (IValueReference), and then serialize
 * the record made up of those fields into a data stream.
 */
public interface IARecordBuilder {

    public void init();

    /**
     * @param recType
     *            The type of the record.
     */
    public void reset(ARecordType recType);

    /**
     * @param id
     *            The field id.
     * @param value
     *            The field value.
     */
    public void addField(int id, IValueReference value);

    /**
     * @param name
     *            The field name.
     * @param value
     *            The field value.
     * @throws HyracksDataException
     *             if the field name conflicts with a closed field name
     */
    public void addField(IValueReference name, IValueReference value) throws HyracksDataException;

    /**
     * @param out
     *            Stream to write data to.
     * @param writeTypeTag
     *            Whether to write a typetag as part of the record's serialized
     *            representation.
     * @throws HyracksDataException
     *             if any open field names conflict with each other
     */
    public void write(DataOutput out, boolean writeTypeTag) throws HyracksDataException;

    public int getFieldId(String fieldName);

}
