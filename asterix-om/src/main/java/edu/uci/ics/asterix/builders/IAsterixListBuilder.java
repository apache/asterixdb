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

import edu.uci.ics.asterix.om.types.AbstractCollectionType;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IValueReference;

public interface IAsterixListBuilder {
    /**
     * @param listType
     *            Type of the list: AUnorderedListType or AOrderedListType.
     */
    public void reset(AbstractCollectionType listType) throws HyracksDataException;

    /**
     * @param item
     *            Item to be added to the list.
     */
    public void addItem(IValueReference item) throws HyracksDataException;

    /**
     * @param out
     *            Stream to serialize the list into.
     * @param writeTypeTag
     *            Whether to write the list's type tag before the list data.
     */
    public void write(DataOutput out, boolean writeTypeTag) throws HyracksDataException;
}
