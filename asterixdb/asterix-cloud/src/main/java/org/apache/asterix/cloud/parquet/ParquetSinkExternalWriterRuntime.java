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
package org.apache.asterix.cloud.parquet;

import static org.apache.asterix.common.exceptions.ErrorCode.EXTERNAL_SINK_ERROR;
import static org.apache.asterix.common.exceptions.ErrorCode.TYPE_UNSUPPORTED_PARQUET_WRITE;
import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.nio.ByteBuffer;

import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.writer.printer.parquet.ParquetSchemaLazyVisitor;
import org.apache.asterix.external.writer.printer.parquet.SchemaCheckerLazyVisitor;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputSinkPushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.writer.IWriterPartitioner;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ParquetSinkExternalWriterRuntime extends AbstractOneInputSinkPushRuntime {
    private final int sourceColumn;
    private final IWriterPartitioner partitioner;
    private final IPointable sourceValue;
    private final ParquetExternalWriterFactory writerFactory;
    private FrameTupleAccessor tupleAccessor;
    private FrameTupleReference tupleRef;
    private IFrameWriter frameWriter;
    private final int maxSchemas;
    private final IAType sourceType;
    private ParquetSchemaInferPoolWriter poolWriter;

    public ParquetSinkExternalWriterRuntime(int sourceColumn, IWriterPartitioner partitioner,
            RecordDescriptor inputRecordDesc, ParquetExternalWriterFactory writerFactory, IAType sourceType,
            int maxSchemas) {
        this.sourceColumn = sourceColumn;
        this.partitioner = partitioner;
        this.sourceValue = new VoidPointable();
        this.inputRecordDesc = inputRecordDesc;
        this.writerFactory = writerFactory;
        this.sourceType = sourceType;
        this.maxSchemas = maxSchemas;
    }

    @Override
    public void setOutputFrameWriter(int index, IFrameWriter frameWriter, RecordDescriptor recordDesc) {
        this.frameWriter = frameWriter;
    }

    @Override
    public void open() throws HyracksDataException {
        if (tupleAccessor == null) {
            tupleAccessor = new FrameTupleAccessor(inputRecordDesc);
            tupleRef = new FrameTupleReference();
        }

        poolWriter = new ParquetSchemaInferPoolWriter(writerFactory, new SchemaCheckerLazyVisitor(sourceType),
                new ParquetSchemaLazyVisitor(sourceType), maxSchemas);
        this.frameWriter.open();

    }

    // Schema Inference is done frame wise, i.e., we infer the schema for all the records in frame and write the values with schema inferred until now.
    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        tupleAccessor.reset(buffer);

        for (int i = 0; i < tupleAccessor.getTupleCount(); i++) {
            tupleRef.reset(tupleAccessor, i);
            setValue(tupleRef, sourceColumn, sourceValue);
            checkWritable(sourceValue);
            poolWriter.inferSchema(sourceValue);
        }

        for (int i = 0; i < tupleAccessor.getTupleCount(); i++) {
            tupleRef.reset(tupleAccessor, i);
            setValue(tupleRef, sourceColumn, sourceValue);
            if (partitioner.isNewPartition(tupleAccessor, i)) {
                // When there is a new partition, we need to close the existing writers.
                poolWriter.closeAll();
            }
            // New files are created on the fly.
            poolWriter.write(sourceValue, tupleRef);
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        if (frameWriter != null) {
            frameWriter.fail();
        }
    }

    // close() runs even when open() failed part-way, so neither field can be assumed to have been assigned.
    @Override
    public void close() throws HyracksDataException {
        Throwable failure = null;
        if (poolWriter != null) {
            try {
                poolWriter.close();
            } catch (Throwable th) { // NOSONAR: the frame writer still has to be closed
                failure = th;
            }
        }
        failure = CleanupUtils.close(frameWriter, failure);
        if (failure != null) {
            throw asSinkFailure(failure);
        }
    }

    /**
     * Failures raised while closing come from finalising the destination files -- flushing the Parquet footer and
     * completing the upload -- so they belong to the external sink rather than to the engine. Wrapped plainly they
     * reach the user as ASX25000 "Internal error", which names nothing they can act on. An exception that already
     * carries an error code keeps it, since that code is more specific than EXTERNAL_SINK_ERROR.
     */
    private static HyracksDataException asSinkFailure(Throwable failure) {
        if (failure instanceof HyracksDataException coded) {
            return coded;
        }
        return RuntimeDataException.create(EXTERNAL_SINK_ERROR, failure, getMessageOrToString(failure));
    }

    /**
     * The source type is a record type, but a nullable one whenever the record is built by a merge, so an unknown
     * value can still reach the writer. Parquet has no representation for a row that is not a record, and the
     * readers below assume the record layout, so refuse it here rather than misreading it.
     */
    private static void checkWritable(IPointable value) throws HyracksDataException {
        ATypeTag tag = ATypeTag.VALUE_TYPE_MAPPING[value.getByteArray()[value.getStartOffset()]];
        if (tag == ATypeTag.NULL || tag == ATypeTag.MISSING) {
            throw RuntimeDataException.create(TYPE_UNSUPPORTED_PARQUET_WRITE, tag);
        }
    }

    private void setValue(IFrameTupleReference tuple, int column, IPointable value) {
        byte[] data = tuple.getFieldData(column);
        int start = tuple.getFieldStart(column);
        int length = tuple.getFieldLength(column);
        value.set(data, start, length);
    }
}
