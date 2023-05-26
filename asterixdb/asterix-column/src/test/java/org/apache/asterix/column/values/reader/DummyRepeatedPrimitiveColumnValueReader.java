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

import java.util.Arrays;
import java.util.List;

import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class DummyRepeatedPrimitiveColumnValueReader extends AbstractDummyColumnValuesReader {
    private final int[] delimiters;
    private final int[] levelToDelimiterMap;
    private int delimiterIndex;

    DummyRepeatedPrimitiveColumnValueReader(ATypeTag typeTag, RunLengthIntArray defLevels, List<IValueReference> values,
            int columnIndex, int maxLevel, int[] delimiters) {
        super(typeTag, defLevels, values, columnIndex, maxLevel);
        this.delimiters = delimiters;
        delimiterIndex = delimiters.length;

        levelToDelimiterMap = new int[maxLevel + 1];
        int currentDelimiterIndex = 0;
        for (int level = maxLevel; level >= 0; level--) {
            if (currentDelimiterIndex < delimiters.length && level == delimiters[currentDelimiterIndex]) {
                currentDelimiterIndex++;
            }
            levelToDelimiterMap[level] = currentDelimiterIndex;
        }
    }

    @Override
    public boolean next() throws HyracksDataException {
        if (valueIndex == valueCount) {
            return false;
        }

        consumeDelimiterIfAny();
        nextLevel();
        setDelimiterIndex();
        if (level == maxLevel) {
            nonMissingValueIndex++;
        }
        valueIndex++;
        return true;
    }

    @Override
    public boolean isRepeated() {
        return true;
    }

    @Override
    public boolean isDelimiter() {
        return delimiterIndex < delimiters.length && level == delimiters[delimiterIndex];
    }

    @Override
    public boolean isLastDelimiter() {
        return isDelimiter() && delimiterIndex == delimiters.length - 1;
    }

    @Override
    public boolean isRepeatedValue() {
        return levelToDelimiterMap[level] < delimiters.length;
    }

    @Override
    public int getDelimiterIndex() {
        return delimiterIndex;
    }

    @Override
    public int getNumberOfDelimiters() {
        return delimiters.length;
    }

    private void consumeDelimiterIfAny() {
        if (isDelimiter()) {
            delimiterIndex++;
        }
    }

    private void setDelimiterIndex() {
        if (isDelimiter() || level <= delimiters[delimiters.length - 1]) {
            return;
        }
        delimiterIndex = levelToDelimiterMap[level];
    }

    @Override
    public void appendReaderInformation(ObjectNode node) {
        appendCommon(node);
        node.put("delimiters", Arrays.toString(delimiters));
        node.put("levelToDelimiterMap", Arrays.toString(levelToDelimiterMap));
        node.put("delimiterIndex", delimiterIndex);
        node.put("isDelimiter", isDelimiter());
    }
}
