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
package org.apache.asterix.external.input.record.reader.aws.iceberg;

import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.DeleteCounter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;

/**
 * A {@link org.apache.iceberg.data.DeleteFilter} over generic {@link Record}s whose required schema never contains the
 * synthetic {@code _pos} (row position) column.
 * <p>
 * This exists because {@link org.apache.iceberg.data.GenericDeleteFilter} hard-codes {@code needRowPosCol = true}, and
 * {@code _pos} is not a column of the Parquet file: it is synthesized by Iceberg's own reader stack. The variant-pruned
 * reader derives its physical projection with {@code ParquetSchemaUtil.pruneColumns}, which can only resolve field ids
 * that actually exist in the file, so a required schema carrying {@code _pos} cannot be read by it at all. Suppressing
 * the column is what lets the pruned read be used on a file that has position deletes or a deletion vector.
 * <p>
 * The positions are not lost — they are obtained the other way round. {@link VariantProjectedParquetReader} takes the
 * {@link org.apache.iceberg.deletes.PositionDeleteIndex} from {@link #deletedRowPositions()} and skips against it
 * directly, using each row group's file-absolute first row index. That is strictly better than materializing a
 * position column: nothing is read, nothing is decoded, and no arithmetic derives a position that the file format
 * already records.
 * <p>
 * Consequently {@link #pos(Record)} is unreachable and throws. Any caller that reaches it has routed a position-delete
 * task into {@code DeleteFilter.filter(..)}, which would otherwise silently dereference a null position accessor; a
 * loud failure there is the point.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Generic delete filter built with needRowPosCol=false so the required schema stays readable by the "
        + "variant-pruned reader; row positions come from the deletion bitmap instead of a materialized column")
final class PositionlessGenericDeleteFilter extends org.apache.iceberg.data.DeleteFilter<Record> {

    private final FileIO io;
    private final InternalRecordWrapper asStructLike;

    PositionlessGenericDeleteFilter(FileIO io, FileScanTask task, Schema tableSchema, Schema requestedSchema) {
        super(task.file().location(), task.deletes(), tableSchema, requestedSchema, new DeleteCounter(), false);
        this.io = io;
        this.asStructLike = new InternalRecordWrapper(requiredSchema().asStruct());
    }

    @Override
    protected long pos(Record record) {
        throw new UnsupportedOperationException(
                "row positions are applied from the deletion bitmap, not from a _pos column");
    }

    @Override
    protected StructLike asStructLike(Record record) {
        return asStructLike.wrap(record);
    }

    @Override
    protected InputFile getInputFile(String location) {
        return io.newInputFile(location);
    }
}
