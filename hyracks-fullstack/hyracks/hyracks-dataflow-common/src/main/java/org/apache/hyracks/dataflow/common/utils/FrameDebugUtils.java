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
package org.apache.hyracks.dataflow.common.utils;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * A Util class used for inspecting frames
 * for debugging purposes
 */
public class FrameDebugUtils {
    private FrameDebugUtils() {
    }

    /**
     * Debugging method
     *
     * @param fta
     * @param recordDescriptor
     * @param prefix
     */
    public static void prettyPrint(IFrameTupleAccessor fta, RecordDescriptor recordDescriptor, String prefix) {
        try (ByteBufferInputStream bbis = new ByteBufferInputStream();
                DataInputStream dis = new DataInputStream(bbis)) {
            int tc = fta.getTupleCount();
            StringBuilder sb = new StringBuilder();
            sb.append(prefix).append("TC: " + tc).append("\n");
            for (int i = 0; i < tc; ++i) {
                prettyPrint(fta, recordDescriptor, i, bbis, dis, sb);
            }
            System.err.println(sb.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Debugging method
     *
     * @param fta
     * @param recordDescriptor
     */
    public static void prettyPrint(IFrameTupleAccessor fta, RecordDescriptor recordDescriptor) {
        prettyPrint(fta, recordDescriptor, "");
    }

    /**
     * Debugging method
     *
     * @param fta
     * @param operator
     */
    public static void prettyPrintTags(IFrameTupleAccessor fta, String operator) {
        try (ByteBufferInputStream bbis = new ByteBufferInputStream();
                DataInputStream dis = new DataInputStream(bbis)) {
            int tc = fta.getTupleCount();
            StringBuilder sb = new StringBuilder();
            sb.append(operator + ":");
            sb.append("TC: " + tc).append("\n");
            for (int i = 0; i < tc; ++i) {
                prettyPrintTag(fta, i, bbis, dis, sb);
            }
            System.err.println(sb.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Debugging method
     *
     * @param fta
     * @param tid
     * @param bbis
     * @param dis
     * @param sb
     */
    protected static void prettyPrintTag(IFrameTupleAccessor fta, int tid, ByteBufferInputStream bbis,
            DataInputStream dis, StringBuilder sb) {
        sb.append(" tid" + tid + ":(" + fta.getTupleStartOffset(tid) + ", " + fta.getTupleEndOffset(tid) + ")[");
        for (int j = 0; j < fta.getFieldCount(); ++j) {
            sb.append(" ");
            if (j > 0) {
                sb.append("|");
            }
            sb.append("f" + j + ":(" + fta.getFieldStartOffset(tid, j) + ", " + fta.getFieldEndOffset(tid, j) + ") ");
            sb.append("{");
            sb.append(Byte.toString(fta.getBuffer().array()[fta.getTupleStartOffset(tid) + fta.getFieldSlotsLength()
                    + fta.getFieldStartOffset(tid, j)]));
            sb.append("}");
        }
        sb.append("\n");
    }

    /**
     * Debugging method
     *
     * @param fta
     * @param recordDescriptor
     * @param tid
     * @param bbis
     * @param dis
     * @param sb
     */
    protected static void prettyPrint(IFrameTupleAccessor fta, RecordDescriptor recordDescriptor, int tid,
            ByteBufferInputStream bbis, DataInputStream dis, StringBuilder sb) {
        sb.append(" tid" + tid + ":(" + fta.getTupleStartOffset(tid) + ", " + fta.getTupleEndOffset(tid) + ")[");
        for (int j = 0; j < fta.getFieldCount(); ++j) {
            sb.append(" ");
            if (j > 0) {
                sb.append("|");
            }
            sb.append("f" + j + ":(" + fta.getFieldStartOffset(tid, j) + ", " + fta.getFieldEndOffset(tid, j) + ") ");
            sb.append("{");
            bbis.setByteBuffer(fta.getBuffer(),
                    fta.getTupleStartOffset(tid) + fta.getFieldSlotsLength() + fta.getFieldStartOffset(tid, j));
            try {
                sb.append(recordDescriptor.getFields()[j].deserialize(dis));
            } catch (Exception e) {
                e.printStackTrace();
                sb.append("Failed to deserialize field" + j);
            }
            sb.append("}");
        }
        sb.append("\n");
    }

    /**
     * Debugging method
     *
     * @param fta
     * @param recordDescriptor
     * @param tid
     */
    public static void prettyPrint(IFrameTupleAccessor fta, RecordDescriptor recordDescriptor, int tid) {
        try (ByteBufferInputStream bbis = new ByteBufferInputStream();
                DataInputStream dis = new DataInputStream(bbis)) {
            StringBuilder sb = new StringBuilder();
            prettyPrint(fta, recordDescriptor, tid, bbis, dis, sb);
            System.err.println(sb.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Debugging method
     * They are safe as they don't print records. Printing records
     * using IserializerDeserializer can print incorrect results or throw exceptions.
     * A better way yet would be to use record pointable.
     *
     * @param fta
     * @param recordDescriptor
     * @param prefix
     * @param recordFields
     * @throws IOException
     */
    public static void prettyPrint(IFrameTupleAccessor fta, RecordDescriptor recordDescriptor, String prefix,
            int[] recordFields) throws IOException {
        try (ByteBufferInputStream bbis = new ByteBufferInputStream();
                DataInputStream dis = new DataInputStream(bbis)) {
            int tc = fta.getTupleCount();
            StringBuilder sb = new StringBuilder();
            sb.append(prefix).append("TC: " + tc).append("\n");
            for (int i = 0; i < tc; ++i) {
                prettyPrint(fta, recordDescriptor, i, bbis, dis, sb, recordFields);
            }
            System.err.println(sb.toString());
        }
    }

    /**
     * Debugging method
     *
     * @param fta
     * @param recordDescriptor
     * @param tIdx
     * @param recordFields
     * @throws IOException
     */
    public static void prettyPrint(IFrameTupleAccessor fta, RecordDescriptor recordDescriptor, int tIdx,
            int[] recordFields) throws IOException {
        try (ByteBufferInputStream bbis = new ByteBufferInputStream();
                DataInputStream dis = new DataInputStream(bbis)) {
            StringBuilder sb = new StringBuilder();
            prettyPrint(fta, recordDescriptor, tIdx, bbis, dis, sb, recordFields);
            System.err.println(sb.toString());
        }
    }

    /**
     * Debugging method
     *
     * @param tuple
     * @param fieldsIdx
     * @param descIdx
     * @throws HyracksDataException
     */
    public static void prettyPrint(IFrameTupleAccessor fta, RecordDescriptor recordDescriptor, ITupleReference tuple,
            int fieldsIdx, int descIdx) throws HyracksDataException {
        try (ByteBufferInputStream bbis = new ByteBufferInputStream();
                DataInputStream dis = new DataInputStream(bbis)) {
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            sb.append("f" + fieldsIdx + ":(" + tuple.getFieldStart(fieldsIdx) + ", "
                    + (tuple.getFieldLength(fieldsIdx) + tuple.getFieldStart(fieldsIdx)) + ") ");
            sb.append("{");
            ByteBuffer bytebuff = ByteBuffer.wrap(tuple.getFieldData(fieldsIdx));
            bbis.setByteBuffer(bytebuff, tuple.getFieldStart(fieldsIdx));
            sb.append(recordDescriptor.getFields()[descIdx].deserialize(dis));
            sb.append("}");
            sb.append("\n");
            System.err.println(sb.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Debugging method
     *
     * @param tuple
     * @param descF
     * @throws HyracksDataException
     */
    public static void prettyPrint(IFrameTupleAccessor fta, RecordDescriptor recordDescriptor, ITupleReference tuple,
            int[] descF) throws HyracksDataException {
        try (ByteBufferInputStream bbis = new ByteBufferInputStream();
                DataInputStream dis = new DataInputStream(bbis)) {
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            for (int j = 0; j < descF.length; ++j) {
                sb.append("f" + j + ":(" + tuple.getFieldStart(j) + ", "
                        + (tuple.getFieldLength(j) + tuple.getFieldStart(j)) + ") ");
                sb.append("{");
                ByteBuffer bytebuff = ByteBuffer.wrap(tuple.getFieldData(j));
                bbis.setByteBuffer(bytebuff, tuple.getFieldStart(j));
                sb.append(recordDescriptor.getFields()[descF[j]].deserialize(dis));
                sb.append("}");
            }
            sb.append("\n");
            System.err.println(sb.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Debugging method
     *
     * @param fta
     * @param recordDescriptor
     * @param tid
     * @param bbis
     * @param dis
     * @param sb
     * @param recordFields
     * @throws IOException
     */
    protected static void prettyPrint(IFrameTupleAccessor fta, RecordDescriptor recordDescriptor, int tid,
            ByteBufferInputStream bbis, DataInputStream dis, StringBuilder sb, int[] recordFields) throws IOException {
        Arrays.sort(recordFields);
        sb.append(" tid" + tid + ":(" + fta.getTupleStartOffset(tid) + ", " + fta.getTupleEndOffset(tid) + ")[");
        for (int j = 0; j < fta.getFieldCount(); ++j) {
            sb.append("f" + j + ":(" + fta.getFieldStartOffset(tid, j) + ", " + fta.getFieldEndOffset(tid, j) + ") ");
            sb.append("{");
            bbis.setByteBuffer(fta.getBuffer(),
                    fta.getTupleStartOffset(tid) + fta.getFieldSlotsLength() + fta.getFieldStartOffset(tid, j));
            if (Arrays.binarySearch(recordFields, j) >= 0) {
                sb.append("{a record field: only print using pointable:");
                sb.append("tag->" + dis.readByte() + "}");
            } else {
                sb.append(recordDescriptor.getFields()[j].deserialize(dis));
            }
            sb.append("}");
        }
        sb.append("\n");
    }
}
