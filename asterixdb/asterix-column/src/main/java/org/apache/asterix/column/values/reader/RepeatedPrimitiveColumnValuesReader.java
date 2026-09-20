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

import org.apache.asterix.column.values.IColumnValuesWriter;
import org.apache.asterix.column.values.reader.value.AbstractValueReader;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.InvokeUtil;
import org.apache.hyracks.storage.am.lsm.btree.column.error.ColumnarValueException;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * For primitive values that belong to an {@link ATypeTag#ARRAY} and {@link ATypeTag#MULTISET}
 */
public final class RepeatedPrimitiveColumnValuesReader extends AbstractColumnValuesReader {
    private final int[] delimiters;
    private final int[] levelToDelimiterMap;
    private int delimiterIndex;

    RepeatedPrimitiveColumnValuesReader(AbstractValueReader valueReader, int columnIndex, int maxLevel,
            int[] delimiters) {
        super(valueReader, columnIndex, maxLevel, false);
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
    protected void resetValues() {
        delimiterIndex = delimiters.length;
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
            valueReader.nextValue();
        }
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
    public int getNumberOfDelimiters() {
        return delimiters.length;
    }

    @Override
    public int getDelimiterIndex() {
        return delimiterIndex;
    }

    @Override
    public void write(IColumnValuesWriter writer, boolean callNext) throws HyracksDataException {
        //We always call next as repeated values cannot be primary keys
        doNextAndCheck();

        if (isRepeatedValue()) {
            while (!isLastDelimiter()) {
                InvokeUtil.failIfInterrupted();
                writeLevel(writer);
                if (isValue()) {
                    writer.writeValue(this);
                }
                doNextAndCheck();
            }
        }

        //Add last delimiter, or NULL/MISSING
        writeLevel(writer);
    }

    @Override
    public boolean isRepeatedValue() {
        return levelToDelimiterMap[level] < delimiters.length;
    }

    @Override
    public boolean isLastDelimiter() {
        return isDelimiter() && delimiterIndex == delimiters.length - 1;
    }

    @Override
    public void skip(int count) throws HyracksDataException {
        int values = 0;
        for (int i = 0; i < count; i++) {
            values += skipTuple();
        }
        valueReader.skip(values);
    }

    /**
     * Consumes one tuple's entries, up to and including its last delimiter, and returns the number of values among
     * them. Runs of plain values are consumed in bulk; only the entries that carry structure (delimiters, nulls,
     * missing) are stepped over one at a time.
     */
    private int skipTuple() throws HyracksDataException {
        int values = skipPlainEntries();
        if (values == 0) {
            values = skipEntry();
            if (!isRepeatedValue()) {
                return values;
            }
        }
        while (!isLastDelimiter()) {
            // the delimiter is the sole exit; inconsistent delimiter state turns this into a pure-CPU spin
            InvokeUtil.failIfInterrupted();
            int plain = skipPlainEntries();
            values += plain > 0 ? plain : skipEntry();
        }
        return values;
    }

    private int skipPlainEntries() throws HyracksDataException {
        int plain = skipPlainValues(valueCount - valueIndex);
        if (plain > 0) {
            // what setDelimiterIndex() computes for an entry at the max level
            delimiterIndex = levelToDelimiterMap[maxLevel];
        }
        return plain;
    }

    /**
     * Steps over a single entry exactly as {@link #next()} does, minus decoding the value; returns 1 if the entry
     * was a value.
     */
    private int skipEntry() throws HyracksDataException {
        if (valueIndex == valueCount) {
            throw exhausted();
        }
        consumeDelimiterIfAny();
        nextLevel();
        setDelimiterIndex();
        return level == maxLevel ? 1 : 0;
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

    private void doNextAndCheck() throws HyracksDataException {
        if (!next()) {
            throw exhausted();
        }
    }

    private ColumnarValueException exhausted() {
        ColumnarValueException e = new ColumnarValueException();
        appendReaderInformation(e.createNode(getClass().getSimpleName()));
        return e;
    }
}
