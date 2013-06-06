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
package edu.uci.ics.hivesterix.serde.parser;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.lazy.LazyShort;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Text;

import edu.uci.ics.hivesterix.serde.lazy.LazyUtils;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public class TextToBinaryTupleParser implements IHiveParser {
    private int[] invertedIndex;
    private int[] fieldEnds;
    private int lastNecessaryFieldIndex;
    private LazySimpleStructObjectInspector inputObjectInspector;
    private List<? extends StructField> fieldRefs;

    public TextToBinaryTupleParser(int[] outputColumnsOffset, ObjectInspector structInspector) {
        int size = 0;
        for (int i = 0; i < outputColumnsOffset.length; i++)
            if (outputColumnsOffset[i] >= 0)
                size++;
        invertedIndex = new int[size];
        for (int i = 0; i < outputColumnsOffset.length; i++)
            if (outputColumnsOffset[i] >= 0) {
                invertedIndex[outputColumnsOffset[i]] = i;
                lastNecessaryFieldIndex = i;
            }
        fieldEnds = new int[outputColumnsOffset.length];
        for (int i = 0; i < fieldEnds.length; i++)
            fieldEnds[i] = 0;
        inputObjectInspector = (LazySimpleStructObjectInspector) structInspector;
        fieldRefs = inputObjectInspector.getAllStructFieldRefs();
    }

    @Override
    public void parse(byte[] bytes, int start, int length, ArrayTupleBuilder tb) throws IOException {
        byte separator = inputObjectInspector.getSeparator();
        boolean lastColumnTakesRest = inputObjectInspector.getLastColumnTakesRest();
        boolean isEscaped = inputObjectInspector.isEscaped();
        byte escapeChar = inputObjectInspector.getEscapeChar();
        DataOutput output = tb.getDataOutput();

        int structByteEnd = start + length - 1;
        int fieldId = 0;
        int fieldByteEnd = start;

        // Go through all bytes in the byte[]
        while (fieldByteEnd <= structByteEnd && fieldId <= lastNecessaryFieldIndex) {
            if (fieldByteEnd == structByteEnd || bytes[fieldByteEnd] == separator) {
                // Reached the end of a field?
                if (lastColumnTakesRest && fieldId == fieldEnds.length - 1) {
                    fieldByteEnd = structByteEnd;
                }
                fieldEnds[fieldId] = fieldByteEnd;
                if (fieldId == fieldEnds.length - 1 || fieldByteEnd == structByteEnd) {
                    // for the case of null fields
                    for (int i = fieldId; i < fieldEnds.length; i++) {
                        fieldEnds[i] = fieldByteEnd;
                    }
                    break;
                }
                fieldByteEnd++;
                fieldId++;
            } else {
                if (isEscaped && bytes[fieldByteEnd] == escapeChar && fieldByteEnd + 1 < structByteEnd) {
                    // ignore the char after escape_char
                    fieldByteEnd += 2;
                } else {
                    fieldByteEnd++;
                }
            }
        }

        for (int i = 0; i < invertedIndex.length; i++) {
            int index = invertedIndex[i];
            StructField fieldRef = fieldRefs.get(index);
            ObjectInspector inspector = fieldRef.getFieldObjectInspector();
            Category category = inspector.getCategory();
            int fieldStart = index == 0 ? 0 : fieldEnds[index - 1] + 1;
            int fieldEnd = fieldEnds[index];
            if (bytes[fieldEnd] == separator)
                fieldEnd--;
            int fieldLen = fieldEnd - fieldStart + 1;
            switch (category) {
                case PRIMITIVE:
                    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) inspector;
                    switch (poi.getPrimitiveCategory()) {
                        case VOID: {
                            break;
                        }
                        case BOOLEAN: {
                            output.write(bytes[fieldStart]);
                            break;
                        }
                        case BYTE: {
                            output.write(bytes[fieldStart]);
                            break;
                        }
                        case SHORT: {
                            short v = LazyShort.parseShort(bytes, fieldStart, fieldLen);
                            output.write((byte) (v >> 8));
                            output.write((byte) (v));
                            break;
                        }
                        case INT: {
                            int v = LazyInteger.parseInt(bytes, fieldStart, fieldLen);
                            LazyUtils.writeVInt(output, v);
                            break;
                        }
                        case LONG: {
                            long v = LazyLong.parseLong(bytes, fieldStart, fieldLen);
                            LazyUtils.writeVLong(output, v);
                            break;
                        }
                        case FLOAT: {
                            float value = Float.parseFloat(Text.decode(bytes, fieldStart, fieldLen));
                            int v = Float.floatToIntBits(value);
                            output.write((byte) (v >> 24));
                            output.write((byte) (v >> 16));
                            output.write((byte) (v >> 8));
                            output.write((byte) (v));
                            break;
                        }
                        case DOUBLE: {
                            try {
                                double value = Double.parseDouble(Text.decode(bytes, fieldStart, fieldLen));
                                long v = Double.doubleToLongBits(value);
                                output.write((byte) (v >> 56));
                                output.write((byte) (v >> 48));
                                output.write((byte) (v >> 40));
                                output.write((byte) (v >> 32));
                                output.write((byte) (v >> 24));
                                output.write((byte) (v >> 16));
                                output.write((byte) (v >> 8));
                                output.write((byte) (v));
                            } catch (NumberFormatException e) {
                                throw e;
                            }
                            break;
                        }
                        case STRING: {
                            LazyUtils.writeVInt(output, fieldLen);
                            output.write(bytes, fieldStart, fieldLen);
                            break;
                        }
                        default: {
                            throw new RuntimeException("Unrecognized type: " + poi.getPrimitiveCategory());
                        }
                    }
                    break;
                case STRUCT:
                    throw new NotImplementedException("Unrecognized type: struct ");
                case LIST:
                    throw new NotImplementedException("Unrecognized type: struct ");
                case MAP:
                    throw new NotImplementedException("Unrecognized type: struct ");
                case UNION:
                    throw new NotImplementedException("Unrecognized type: struct ");
            }
            tb.addFieldEndOffset();
        }
    }
}
