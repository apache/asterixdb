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
package org.apache.asterix.runtime.evaluators.common;

import org.apache.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public final class UnorderedListIterator extends AbstractAsterixListIterator {

    @Override
    protected int getItemOffset(byte[] serOrderedList, int offset, int itemIndex) throws HyracksDataException {
        return AUnorderedListSerializerDeserializer.getItemOffset(serOrderedList, offset, itemIndex);
    }

    @Override
    protected int getNumberOfItems(byte[] serOrderedList, int offset) {
        return AUnorderedListSerializerDeserializer.getNumberOfItems(serOrderedList, offset);
    }

    @Override
    protected int getListLength(byte[] serOrderedList, int offset) {
        return AUnorderedListSerializerDeserializer.getUnorderedListLength(serOrderedList, offset + 1);
    }
}
