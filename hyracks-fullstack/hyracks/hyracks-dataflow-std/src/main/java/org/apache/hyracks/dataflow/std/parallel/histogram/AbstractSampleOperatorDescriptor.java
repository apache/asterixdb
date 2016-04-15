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

package org.apache.hyracks.dataflow.std.parallel.histogram;

import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.parallel.HistogramAlgorithm;
import org.apache.hyracks.dataflow.std.parallel.base.MaterializingSampleTaskState;

/**
 * @author michael
 * @Comments: This Descriptor can be seen as the local sampling method for the uniform, random and wavelet based samplers plus the
 *            materialization for both the sample and the input dataset, the merge part of the sampler is given in the following
 *            AbstractSampleMergeOperatorDescriptor after a MToOneExchange.
 */
public abstract class AbstractSampleOperatorDescriptor extends AbstractOperatorDescriptor {

    //    private static final Logger LOGGER = Logger.getLogger(AbstractSampleOperatorDescriptor.class.getName());

    private static final long serialVersionUID = 1L;

    private final static int LOCAL_SAMPLING_FACTOR = 2;

    private final static int MATER_SAMPLER_ACTIVITY_ID = 0;
    private final static int MATER_READER_ACTIVITY_ID = 1;

    // sampleMaterializationFlags numbers the samples plus the count of the materialization count
    private final HistogramAlgorithm alg;
    private boolean[] sampleMaterializationFlags;
    private boolean requiresMaterialization;
    private int numberOfNonMaterializedOutputs = 0;
    private int numberOfActiveMaterializeReaders = 0;

    private final int[] sampleFields;
    private final int sampleBasis;
    private IBinaryComparatorFactory[] comparatorFactories;

    private RecordDescriptor outDesc;
    private RecordDescriptor inDesc;

    public AbstractSampleOperatorDescriptor(IOperatorDescriptorRegistry spec, int frameLimit, int[] sampleFields,
            int sampleBasis, RecordDescriptor rDesc, IBinaryComparatorFactory[] compFactories, HistogramAlgorithm alg,
            int outputArity) {
        this(spec, frameLimit, sampleFields, sampleBasis, rDesc, compFactories, alg, outputArity,
                new boolean[outputArity]);
    }

    public AbstractSampleOperatorDescriptor(IOperatorDescriptorRegistry spec, int frameLimit, int[] sampleFields,
            int sampleBasis, RecordDescriptor rDesc, IBinaryComparatorFactory[] compFactories, HistogramAlgorithm alg,
            int outputArity, boolean[] outputMaterializationFlags) {
        super(spec, 1, outputArity + 1);
        // Column 0 for sampling column(s) for feeding the range of those joinable operators. length for sampleKey point, 1 for count
        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] outSchema = new ISerializerDeserializer[sampleFields.length + 1];
        for (int i = 0; i < sampleFields.length; i++) {
            outSchema[i] = rDesc.getFields()[sampleFields[i]];
        }
        outSchema[outSchema.length - 1] = IntegerSerializerDeserializer.INSTANCE;
        this.outDesc = new RecordDescriptor(outSchema);
        this.inDesc = rDesc;
        if (outputArity <= 0) {
            recordDescriptors[0] = outDesc;
        } else {
            recordDescriptors[0] = outDesc;
            for (int i = 1; i <= outputArity; i++) {
                recordDescriptors[i] = rDesc;
            }
        }
        // this.sampleMaterializationFlags = outputMaterializationFlags;
        this.sampleMaterializationFlags = new boolean[outputMaterializationFlags.length + 1];
        this.sampleMaterializationFlags[0] = false;
        System.arraycopy(outputMaterializationFlags, 0, this.sampleMaterializationFlags, 1, outputArity);
        this.sampleFields = sampleFields;
        // sampleBasis is desired by the UC numbers * GLOBAL_FACTOR * LOCAL_SMPLEING_FACTOR, while the LOCAL is merged in total.
        // The actual ranges need to merge is MOST LIKELY sampleBasis * NC numbers and will be detailed by sampling algorithms.
        this.sampleBasis = sampleBasis * LOCAL_SAMPLING_FACTOR;
        this.comparatorFactories = compFactories;
        this.requiresMaterialization = false;
        for (boolean flag : this.sampleMaterializationFlags) {
            if (flag) {
                this.requiresMaterialization = true;
                break;
            }
        }
        if (null == alg)
            this.alg = HistogramAlgorithm.ORDERED_HISTOGRAM;
        else
            this.alg = alg;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        MaterializingSamplerActivityNode msa = new MaterializingSamplerActivityNode(new ActivityId(odId,
                MATER_SAMPLER_ACTIVITY_ID));
        builder.addActivity(this, msa);
        builder.addSourceEdge(0, msa, 0);
        int outputIndex = 0;

        for (int i = 0; i < outputArity; i++) {
            if (!sampleMaterializationFlags[i]) {
                builder.addTargetEdge(i, msa, outputIndex);
                outputIndex++;
            }
        }

        numberOfNonMaterializedOutputs = outputIndex;
        int activityId = MATER_READER_ACTIVITY_ID;
        for (int i = 0; i < outputArity; i++) {
            if (sampleMaterializationFlags[i]) {
                MaterializedSampleReaderActivityNode msra = new MaterializedSampleReaderActivityNode(new ActivityId(
                        odId, activityId));
                builder.addActivity(this, msra);
                // builder.addSourceEdge(1, msra, 0);
                builder.addTargetEdge(i, msra, 0);
                builder.addBlockingEdge(msa, msra);
                activityId++;
                outputIndex++;
                numberOfActiveMaterializeReaders++;
            }
        }
    }

    private class MaterializingSamplerActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public MaterializingSamplerActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
                throws HyracksDataException {
            return new MaterializingSampleOperatorNodePushable(ctx, new TaskId(getActivityId(), partition),
                    sampleFields, sampleBasis, comparatorFactories, alg, inDesc, outDesc,
                    numberOfNonMaterializedOutputs, requiresMaterialization, partition);
        }
    }

    private class MaterializedSampleReaderActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public MaterializedSampleReaderActivityNode(ActivityId id) {
            super(id);
            // TODO Auto-generated constructor stub
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
                throws HyracksDataException {
            return new AbstractUnaryOutputSourceOperatorNodePushable() {
                @Override
                public void initialize() throws HyracksDataException {
                    MaterializingSampleTaskState state = (MaterializingSampleTaskState) ctx.getStateObject(new TaskId(
                            new ActivityId(getOperatorId(), MATER_SAMPLER_ACTIVITY_ID), partition));
                    state.writeOut(writer, new VSizeFrame(ctx));
                }

                @Override
                public void deinitialize() throws HyracksDataException {
                    numberOfActiveMaterializeReaders--;
                    MaterializingSampleTaskState state = (MaterializingSampleTaskState) ctx.getStateObject(new TaskId(
                            new ActivityId(getOperatorId(), MATER_SAMPLER_ACTIVITY_ID), partition));
                    if (numberOfActiveMaterializeReaders == 0)
                        state.deleteFile();
                }
            };
        }
    }
}
