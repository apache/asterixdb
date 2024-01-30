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
package org.apache.hyracks.algebricks.runtime.operators.writer;

import org.apache.hyracks.algebricks.runtime.base.IPushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.writers.IExternalWriter;
import org.apache.hyracks.algebricks.runtime.writers.IExternalWriterFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public final class SinkExternalWriterRuntimeFactory extends AbstractPushRuntimeFactory {
    private static final long serialVersionUID = -2215789207336628581L;
    private final int sourceColumn;
    private final int[] partitionColumns;
    private final IBinaryComparatorFactory[] partitionComparatorFactories;
    private final boolean partitionByKey;
    private final RecordDescriptor inputRecordDescriptor;
    private final IExternalWriterFactory writerFactory;

    public SinkExternalWriterRuntimeFactory(int sourceColumn, int[] partitionColumns,
            IBinaryComparatorFactory[] partitionComparatorFactories, RecordDescriptor inputRecordDescriptor,
            IExternalWriterFactory writerFactory) {
        this(sourceColumn, partitionColumns, partitionComparatorFactories, false, inputRecordDescriptor, writerFactory);
    }

    public SinkExternalWriterRuntimeFactory(int sourceColumn, RecordDescriptor inputRecordDescriptor,
            IExternalWriterFactory writerFactory) {
        this(sourceColumn, null, null, true, inputRecordDescriptor, writerFactory);
    }

    private SinkExternalWriterRuntimeFactory(int sourceColumn, int[] partitionColumns,
            IBinaryComparatorFactory[] partitionComparatorFactories, boolean partitionByKey,
            RecordDescriptor inputRecordDescriptor, IExternalWriterFactory writerFactory) {
        this.sourceColumn = sourceColumn;
        this.partitionColumns = partitionColumns;
        this.partitionComparatorFactories = partitionComparatorFactories;
        this.partitionByKey = partitionByKey;
        this.inputRecordDescriptor = inputRecordDescriptor;
        this.writerFactory = writerFactory;
    }

    @Override
    public IPushRuntime[] createPushRuntime(IHyracksTaskContext ctx) throws HyracksDataException {
        IExternalWriter writer = writerFactory.createWriter(ctx);
        SinkExternalWriterRuntime runtime =
                new SinkExternalWriterRuntime(sourceColumn, createPartitioner(), inputRecordDescriptor, writer);
        return new IPushRuntime[] { runtime };
    }

    /**
     * Creates the writer partitioner based on the provided parameters
     *
     * @return writer partitioner
     */
    private IWriterPartitioner createPartitioner() {
        // key writer partitioner
        if (partitionByKey) {
            return KeyWriterPartitioner.INSTANCE;
        }

        // writer partitioner
        if (partitionColumns.length > 0) {
            IBinaryComparator[] comparators = new IBinaryComparator[partitionComparatorFactories.length];
            for (int i = 0; i < partitionComparatorFactories.length; i++) {
                comparators[i] = partitionComparatorFactories[i].createBinaryComparator();
            }

            return new WriterPartitioner(partitionColumns, comparators);
        }

        // no-op partitioner
        return new NoOpWriterPartitioner();
    }
}
