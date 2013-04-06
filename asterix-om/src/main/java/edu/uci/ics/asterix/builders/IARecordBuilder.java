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

package edu.uci.ics.asterix.builders;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.data.std.api.IValueReference;

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
     * @param out
     *            The field value.
     * @throws AsterixException
     *             if the field name conflicts with a closed field name
     */
    public void addField(IValueReference name, IValueReference value) throws AsterixException;

    /**
     * @param out
     *            Stream to write data to.
     * @param writeTypeTag
     *            Whether to write a typetag as part of the record's serialized
     *            representation.
     * @throws IOException
     * @throws AsterixException
     *             if any open field names conflict with each other
     */
    public void write(DataOutput out, boolean writeTypeTag) throws IOException, AsterixException;

    public int getFieldId(String fieldName);

}