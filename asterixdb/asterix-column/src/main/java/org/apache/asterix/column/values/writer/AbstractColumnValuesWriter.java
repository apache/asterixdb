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
package org.apache.asterix.column.values.writer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.asterix.column.bytes.encoder.ParquetRunLengthBitPackingHybridEncoder;
import org.apache.asterix.column.util.ColumnValuesUtil;
import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.IColumnValuesWriter;
import org.apache.asterix.column.values.IColumnValuesWriterFactory;
import org.apache.asterix.column.values.writer.filters.AbstractColumnFilterWriter;
import org.apache.asterix.column.values.writer.filters.NoOpColumnFilterWriter;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;

public abstract class AbstractColumnValuesWriter implements IColumnValuesWriter {
    private static final Logger LOGGER = LogManager.getLogger();
    // For 3 integers (count, defSize, and valueSize)
    private static final int COUNT_DEF_SIZE_VALUE_SIZE = Integer.BYTES * 3;
    protected final AbstractColumnFilterWriter filterWriter;
    protected final ParquetRunLengthBitPackingHybridEncoder definitionLevels;
    protected final int level;

    private final int columnIndex;
    private final boolean collection;
    private final int nullBitMask;
    private int count;
    private boolean writeValues;

    AbstractColumnValuesWriter(int columnIndex, int level, boolean collection, boolean filtered) {
        this.columnIndex = columnIndex;
        this.level = level;
        this.collection = collection;
        nullBitMask = ColumnValuesUtil.getNullMask(level);
        int width = ColumnValuesUtil.getBitWidth(level);
        definitionLevels = new ParquetRunLengthBitPackingHybridEncoder(width);
        this.filterWriter = filtered ? createFilter() : NoOpColumnFilterWriter.INSTANCE;
    }

    @Override
    public final int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public final int getEstimatedSize() {
        return definitionLevels.getEstimatedSize() + getValuesEstimatedSize() + COUNT_DEF_SIZE_VALUE_SIZE;
    }

    @Override
    public int getEstimatedSize(int length) {
        // + 1 byte for the definition level
        return Byte.BYTES + calculateEstimatedSize(length);
    }

    @Override
    public final int getAllocatedSpace() {
        return definitionLevels.getAllocatedSize() + getValuesAllocatedSize();
    }

    @Override
    public final int getCount() {
        return count;
    }

    @Override
    public final void writeValue(ATypeTag tag, IValueReference value) throws HyracksDataException {
        addLevel(level);
        try {
            addValue(tag, value);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public final void writeLevel(int level) throws HyracksDataException {
        addLevel(level);
    }

    @Override
    public void writeLevels(int level, int count) throws HyracksDataException {
        writeValues = writeValues || this.level == level;
        this.count += count;
        try {
            for (int i = 0; i < count; i++) {
                definitionLevels.writeInt(level);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public RunLengthIntArray getDefinitionLevelsIntArray() {
        return null;
    }

    @Override
    public final void writeNull(int level) throws HyracksDataException {
        addLevel(level | nullBitMask);
    }

    @Override
    public void writeValue(IColumnValuesReader reader) throws HyracksDataException {
        try {
            addValue(reader);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void writeAntiMatter(ATypeTag tag, IValueReference value) throws HyracksDataException {
        addLevel(0);
        try {
            addValue(tag, value);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public final void close() {
        definitionLevels.close();
        closeValues();
    }

    @Override
    public final long getNormalizedMinValue() {
        if (!writeValues) {
            // ignore values as everything is missing/null
            return Long.MAX_VALUE;
        }
        return filterWriter.getMinNormalizedValue();
    }

    @Override
    public final long getNormalizedMaxValue() {
        if (!writeValues) {
            // ignore values as everything is missing/null
            return Long.MIN_VALUE;
        }
        return filterWriter.getMaxNormalizedValue();
    }

    @Override
    public final void flush(OutputStream out) throws HyracksDataException {
        BytesInput values;
        BytesInput defLevelBytes;
        try {
            BytesUtils.writeZigZagVarInt(level, out);
            defLevelBytes = definitionLevels.toBytes();
            BytesUtils.writeZigZagVarInt((int) defLevelBytes.size(), out);
            BytesUtils.writeZigZagVarInt(count, out);
            defLevelBytes.writeAllTo(out);
            if (writeValues || collection) {
                values = getBytes();
                int valueSize = (int) values.size();
                BytesUtils.writeZigZagVarInt(valueSize, out);
                values.writeAllTo(out);
            } else {
                /*
                 * Do not write the values if all values are null/missing
                 */
                BytesUtils.writeZigZagVarInt(0, out);
            }
        } catch (Exception e) {
            LOGGER.error("Error while flushing columnIndex {}", columnIndex);
            throw HyracksDataException.create(e);
        }
        reset();
    }

    @Override
    public final void reset() throws HyracksDataException {
        definitionLevels.reset();
        writeValues = false;
        count = 0;
        filterWriter.reset();
        resetValues();
    }

    @Override
    public final void serialize(DataOutput output) throws IOException {
        output.write(getTypeTag().serialize());
        output.writeInt(columnIndex);
        output.writeInt(level);
        output.writeBoolean(collection);
        output.writeBoolean(filterWriter != NoOpColumnFilterWriter.INSTANCE);
    }

    public static IColumnValuesWriter deserialize(DataInput input, IColumnValuesWriterFactory writerFactory)
            throws IOException {
        ATypeTag typeTag = ATypeTag.VALUE_TYPE_MAPPING[input.readByte()];
        int columnIndex = input.readInt();
        int level = input.readInt();
        boolean collection = input.readBoolean();
        boolean filtered = input.readBoolean();
        return writerFactory.createValueWriter(typeTag, columnIndex, level, collection, filtered);
    }

    protected void addLevel(int level) throws HyracksDataException {
        try {
            writeValues = writeValues || this.level == level;
            definitionLevels.writeInt(level);
            count++;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    protected abstract ATypeTag getTypeTag();

    protected abstract void addValue(ATypeTag tag, IValueReference value) throws IOException;

    protected abstract void addValue(IColumnValuesReader reader) throws IOException;

    protected abstract BytesInput getBytes() throws IOException;

    protected abstract int getValuesEstimatedSize();

    protected abstract int calculateEstimatedSize(int length);

    protected abstract int getValuesAllocatedSize();

    protected abstract AbstractColumnFilterWriter createFilter();

    protected abstract void resetValues() throws HyracksDataException;

    protected abstract void closeValues();
}
