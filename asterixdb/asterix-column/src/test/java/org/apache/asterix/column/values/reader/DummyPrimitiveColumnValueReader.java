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
package org.apache.asterix.column.values.reader;

import java.util.List;

import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class DummyPrimitiveColumnValueReader extends AbstractDummyColumnValuesReader {
    DummyPrimitiveColumnValueReader(ATypeTag typeTag, RunLengthIntArray defLevels, List<IValueReference> values,
            int columnIndex, int maxLevel) {
        super(typeTag, defLevels, values, columnIndex, maxLevel);
    }

    @Override
    public boolean next() throws HyracksDataException {
        if (valueIndex == valueCount) {
            return false;
        }
        valueIndex++;
        nextLevel();
        if (level == maxLevel) {
            nonMissingValueIndex++;
        }
        return true;
    }

    @Override
    public boolean isRepeated() {
        return false;
    }

    @Override
    public boolean isDelimiter() {
        return false;
    }

    @Override
    public boolean isLastDelimiter() {
        return false;
    }

    @Override
    public boolean isRepeatedValue() {
        return false;
    }

    @Override
    public int getDelimiterIndex() {
        throw new IllegalStateException("Not a repeated reader");
    }

    @Override
    public int getNumberOfDelimiters() {
        return 0;
    }

    @Override
    public void appendReaderInformation(ObjectNode node) {
        appendCommon(node);
        node.put("isPrimaryKeyColumn", false);
    }
}
