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
package org.apache.asterix.metadata.utils;

import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.dataflow.data.nontagged.MissingWriterFactory;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.jobgen.impl.ConnectorPolicyAssignmentPolicy;
import org.apache.hyracks.algebricks.data.ISerializerDeserializerProvider;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.SinkRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.accessors.ShortBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.ShortSerializerDeserializer;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.dataflow.BinaryTokenizerOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;

public class SecondaryCorrelatedInvertedIndexOperationsHelper extends SecondaryCorrelatedTreeIndexOperationsHelper {

    private IAType secondaryKeyType;
    private ITypeTraits[] invListsTypeTraits;
    private IBinaryComparatorFactory[] tokenComparatorFactories;
    private ITypeTraits[] tokenTypeTraits;
    private IBinaryTokenizerFactory tokenizerFactory;
    // For tokenization, sorting and loading. Represents <token, primary keys>.
    private int numTokenKeyPairFields;
    private IBinaryComparatorFactory[] tokenKeyPairComparatorFactories;
    private RecordDescriptor tokenKeyPairRecDesc;
    private boolean isPartitioned;
    private int[] invertedIndexFields;
    private int[] invertedIndexFieldsForNonBulkLoadOps;
    private int[] secondaryFilterFieldsForNonBulkLoadOps;

    protected SecondaryCorrelatedInvertedIndexOperationsHelper(Dataset dataset, Index index,
            MetadataProvider metadataProvider, SourceLocation sourceLoc) throws AlgebricksException {
        super(dataset, index, metadataProvider, sourceLoc);
    }

    @Override
    protected void setSecondaryRecDescAndComparators() throws AlgebricksException {
        int numSecondaryKeys = index.getKeyFieldNames().size();
        IndexType indexType = index.getIndexType();
        boolean isOverridingKeyFieldTypes = index.isOverridingKeyFieldTypes();
        // Sanity checks.
        if (numPrimaryKeys > 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_INDEX_FOR_DATASET_WITH_COMPOSITE_PRIMARY_INDEX,
                    sourceLoc, indexType,
                    RecordUtil.toFullyQualifiedName(dataset.getDataverseName(), dataset.getDatasetName()));
        }
        if (numSecondaryKeys > 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_INDEX_NUM_OF_FIELD, sourceLoc,
                    numSecondaryKeys, indexType, 1);
        }
        if (indexType == IndexType.LENGTH_PARTITIONED_WORD_INVIX
                || indexType == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX) {
            isPartitioned = true;
        } else {
            isPartitioned = false;
        }
        // Prepare record descriptor used in the assign op, and the optional
        // select op.
        secondaryFieldAccessEvalFactories = new IScalarEvaluatorFactory[numSecondaryKeys + numFilterFields];
        ISerializerDeserializer[] secondaryRecFields =
                new ISerializerDeserializer[numPrimaryKeys + numSecondaryKeys + numFilterFields];
        ISerializerDeserializer[] enforcedRecFields = new ISerializerDeserializer[1 + numPrimaryKeys + numFilterFields];
        secondaryTypeTraits = new ITypeTraits[numSecondaryKeys + numPrimaryKeys];
        ITypeTraits[] enforcedTypeTraits = new ITypeTraits[1 + numPrimaryKeys];
        ISerializerDeserializerProvider serdeProvider = metadataProvider.getDataFormat().getSerdeProvider();
        ITypeTraitProvider typeTraitProvider = metadataProvider.getDataFormat().getTypeTraitProvider();
        int recordColumn = NUM_TAG_FIELDS + numPrimaryKeys;
        if (numSecondaryKeys > 0) {
            secondaryFieldAccessEvalFactories[0] = metadataProvider.getDataFormat().getFieldAccessEvaluatorFactory(
                    metadataProvider.getFunctionManager(), isOverridingKeyFieldTypes ? enforcedItemType : itemType,
                    index.getKeyFieldNames().get(0), recordColumn, sourceLoc);
            Pair<IAType, Boolean> keyTypePair = Index.getNonNullableOpenFieldType(index.getKeyFieldTypes().get(0),
                    index.getKeyFieldNames().get(0), itemType);
            secondaryKeyType = keyTypePair.first;
            anySecondaryKeyIsNullable = anySecondaryKeyIsNullable || keyTypePair.second;
            ISerializerDeserializer keySerde = serdeProvider.getSerializerDeserializer(secondaryKeyType);
            secondaryRecFields[0] = keySerde;
            secondaryTypeTraits[0] = typeTraitProvider.getTypeTrait(secondaryKeyType);
        }
        if (numFilterFields > 0) {
            secondaryFieldAccessEvalFactories[numSecondaryKeys] =
                    metadataProvider.getDataFormat().getFieldAccessEvaluatorFactory(
                            metadataProvider.getFunctionManager(), itemType, filterFieldName, recordColumn, sourceLoc);
            Pair<IAType, Boolean> keyTypePair = Index.getNonNullableKeyFieldType(filterFieldName, itemType);
            IAType type = keyTypePair.first;
            ISerializerDeserializer serde = serdeProvider.getSerializerDeserializer(type);
            secondaryRecFields[numPrimaryKeys + numSecondaryKeys] = serde;
        }
        secondaryRecDesc = new RecordDescriptor(secondaryRecFields);
        // Comparators and type traits for tokens.
        int numTokenFields = (!isPartitioned) ? numSecondaryKeys : numSecondaryKeys + 1;
        tokenComparatorFactories = new IBinaryComparatorFactory[numTokenFields];
        tokenTypeTraits = new ITypeTraits[numTokenFields];
        tokenComparatorFactories[0] = NonTaggedFormatUtil.getTokenBinaryComparatorFactory(secondaryKeyType);
        tokenTypeTraits[0] = NonTaggedFormatUtil.getTokenTypeTrait(secondaryKeyType);
        if (isPartitioned) {
            // The partitioning field is hardcoded to be a short *without* an Asterix type tag.
            tokenComparatorFactories[1] = ShortBinaryComparatorFactory.INSTANCE;
            tokenTypeTraits[1] = ShortPointable.TYPE_TRAITS;
        }
        // Set tokenizer factory.
        // TODO: We might want to expose the hashing option at the AQL level,
        // and add the choice to the index metadata.
        tokenizerFactory = NonTaggedFormatUtil.getBinaryTokenizerFactory(secondaryKeyType.getTypeTag(), indexType,
                index.getGramLength());
        // Type traits for inverted-list elements. Inverted lists contain
        // primary keys.
        invListsTypeTraits = new ITypeTraits[numPrimaryKeys];
        if (numPrimaryKeys > 0) {
            invListsTypeTraits[0] = primaryRecDesc.getTypeTraits()[0];
            enforcedRecFields[0] = primaryRecDesc.getFields()[0];
            enforcedTypeTraits[0] = primaryRecDesc.getTypeTraits()[0];
        }
        enforcedRecFields[numPrimaryKeys] = serdeProvider.getSerializerDeserializer(itemType);
        enforcedRecDesc = new RecordDescriptor(enforcedRecFields, enforcedTypeTraits);
        // For tokenization, sorting and loading.
        // One token (+ optional partitioning field) + primary keys.
        numTokenKeyPairFields = (!isPartitioned) ? 1 + numPrimaryKeys : 2 + numPrimaryKeys;
        ISerializerDeserializer[] tokenKeyPairFields =
                new ISerializerDeserializer[numTokenKeyPairFields + numFilterFields];
        ITypeTraits[] tokenKeyPairTypeTraits = new ITypeTraits[numTokenKeyPairFields];
        tokenKeyPairComparatorFactories = new IBinaryComparatorFactory[numTokenKeyPairFields];
        tokenKeyPairFields[0] = serdeProvider.getSerializerDeserializer(secondaryKeyType);
        tokenKeyPairTypeTraits[0] = tokenTypeTraits[0];
        tokenKeyPairComparatorFactories[0] = NonTaggedFormatUtil.getTokenBinaryComparatorFactory(secondaryKeyType);
        int pkOff = 1;
        if (isPartitioned) {
            tokenKeyPairFields[1] = ShortSerializerDeserializer.INSTANCE;
            tokenKeyPairTypeTraits[1] = tokenTypeTraits[1];
            tokenKeyPairComparatorFactories[1] = ShortBinaryComparatorFactory.INSTANCE;
            pkOff = 2;
        }
        if (numPrimaryKeys > 0) {
            tokenKeyPairFields[pkOff] = primaryRecDesc.getFields()[0];
            tokenKeyPairTypeTraits[pkOff] = primaryRecDesc.getTypeTraits()[0];
            tokenKeyPairComparatorFactories[pkOff] = primaryComparatorFactories[0];
        }
        if (numFilterFields > 0) {
            tokenKeyPairFields[numPrimaryKeys + pkOff] = secondaryRecFields[numPrimaryKeys + numSecondaryKeys];
        }
        tokenKeyPairRecDesc = new RecordDescriptor(tokenKeyPairFields, tokenKeyPairTypeTraits);
        if (filterFieldName != null) {
            invertedIndexFields = new int[numTokenKeyPairFields];
            for (int i = 0; i < invertedIndexFields.length; i++) {
                invertedIndexFields[i] = i;
            }
            secondaryFilterFieldsForNonBulkLoadOps = new int[numFilterFields];
            secondaryFilterFieldsForNonBulkLoadOps[0] = numSecondaryKeys + numPrimaryKeys;
            invertedIndexFieldsForNonBulkLoadOps = new int[numSecondaryKeys + numPrimaryKeys];
            for (int i = 0; i < invertedIndexFieldsForNonBulkLoadOps.length; i++) {
                invertedIndexFieldsForNonBulkLoadOps[i] = i;
            }
        }
    }

    @Override
    protected int getNumSecondaryKeys() {
        return numTokenKeyPairFields - numPrimaryKeys;
    }

    @Override
    public JobSpecification buildLoadingJobSpec() throws AlgebricksException {
        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        IndexUtil.bindJobEventListener(spec, metadataProvider);

        // Create dummy key provider for feeding the primary index scan.
        IOperatorDescriptor keyProviderOp = DatasetUtil.createDummyKeyProviderOp(spec, dataset, metadataProvider);

        // Create primary index scan op.
        IOperatorDescriptor primaryScanOp = createPrimaryIndexScanDiskComponentsOp(spec, metadataProvider,
                getTaggedRecordDescriptor(dataset.getPrimaryRecordDescriptor(metadataProvider)));

        IOperatorDescriptor sourceOp = primaryScanOp;
        boolean isOverridingKeyFieldTypes = index.isOverridingKeyFieldTypes();
        int numSecondaryKeys = index.getKeyFieldNames().size();
        if (isOverridingKeyFieldTypes && !enforcedItemType.equals(itemType)) {
            sourceOp = createCastOp(spec, dataset.getDatasetType(), index.isEnforced());
            spec.connect(new OneToOneConnectorDescriptor(spec), primaryScanOp, 0, sourceOp, 0);
        }

        RecordDescriptor taggedSecondaryRecDesc = getTaggedRecordDescriptor(secondaryRecDesc);

        RecordDescriptor taggedTokenKeyPairRecDesc = getTaggedRecordDescriptor(tokenKeyPairRecDesc);

        AlgebricksMetaOperatorDescriptor asterixAssignOp =
                createAssignOp(spec, numSecondaryKeys, taggedSecondaryRecDesc);

        // Generate compensate tuples for upsert
        IOperatorDescriptor processorOp =
                createTupleProcessorOp(spec, taggedSecondaryRecDesc, numSecondaryKeys, numPrimaryKeys, true);

        // Create a tokenizer op.
        AbstractOperatorDescriptor tokenizerOp = createTokenizerOp(spec);

        // Create a sort op
        ExternalSortOperatorDescriptor sortOp = createSortOp(spec,
                getTaggedSecondaryComparatorFactories(tokenKeyPairComparatorFactories), taggedTokenKeyPairRecDesc);

        // Create secondary inverted index bulk load op.
        AbstractSingleActivityOperatorDescriptor invIndexBulkLoadOp =
                createTreeIndexBulkLoadOp(spec, metadataProvider, taggedTokenKeyPairRecDesc,
                        createFieldPermutationForBulkLoadOp(), getNumSecondaryKeys(), numPrimaryKeys, true);

        SinkRuntimeFactory sinkRuntimeFactory = new SinkRuntimeFactory();
        sinkRuntimeFactory.setSourceLocation(sourceLoc);
        AlgebricksMetaOperatorDescriptor metaOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 0,
                new IPushRuntimeFactory[] { sinkRuntimeFactory }, new RecordDescriptor[] {});
        metaOp.setSourceLocation(sourceLoc);
        // Connect the operators.
        spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, primaryScanOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, asterixAssignOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, processorOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), processorOp, 0, tokenizerOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), tokenizerOp, 0, sortOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), sortOp, 0, invIndexBulkLoadOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), invIndexBulkLoadOp, 0, metaOp, 0);
        spec.addRoot(metaOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }

    private AbstractOperatorDescriptor createTokenizerOp(JobSpecification spec) throws AlgebricksException {
        int docField = NUM_TAG_FIELDS;
        int numSecondaryKeys = index.getKeyFieldNames().size();
        int[] keyFields = new int[NUM_TAG_FIELDS + numPrimaryKeys + numFilterFields];
        // set tag fields
        for (int i = 0; i < NUM_TAG_FIELDS; i++) {
            keyFields[i] = i;
        }
        // set primary key + filter fields
        for (int i = NUM_TAG_FIELDS; i < keyFields.length; i++) {
            keyFields[i] = i + numSecondaryKeys;
        }
        BinaryTokenizerOperatorDescriptor tokenizerOp = new BinaryTokenizerOperatorDescriptor(spec,
                getTaggedRecordDescriptor(tokenKeyPairRecDesc), tokenizerFactory, docField, keyFields, isPartitioned,
                false, true, MissingWriterFactory.INSTANCE);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, tokenizerOp,
                primaryPartitionConstraint);
        return tokenizerOp;
    }

    @Override
    protected ExternalSortOperatorDescriptor createSortOp(JobSpecification spec,
            IBinaryComparatorFactory[] taggedSecondaryComparatorFactories, RecordDescriptor taggedSecondaryRecDesc) {
        /**
         * after tokenization, the field layout becomes
         * [token, num?, tag, primary key, filter value]
         * we need to sort on
         * [tag, token, num?, primary key]
         */
        int[] taggedSortFields = new int[taggedSecondaryComparatorFactories.length];
        int numSecondaryKeys = getNumSecondaryKeys();
        int idx = 0;
        // set component pos fields
        taggedSortFields[idx++] = numSecondaryKeys;
        // set secondary keys
        for (int i = 0; i < numSecondaryKeys; i++) {
            taggedSortFields[idx++] = i;
        }
        // set primary keys
        for (int i = 0; i < numPrimaryKeys; i++) {
            taggedSortFields[idx++] = i + numSecondaryKeys + NUM_TAG_FIELDS;
        }
        ExternalSortOperatorDescriptor sortOp = new ExternalSortOperatorDescriptor(spec, sortNumFrames,
                taggedSortFields, taggedSecondaryComparatorFactories, taggedSecondaryRecDesc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, sortOp, primaryPartitionConstraint);
        return sortOp;
    }

    @Override
    protected int[] createFieldPermutationForBulkLoadOp() {
        /**
         * after tokenization, the field layout becomes
         * [token, num?, tag, primary key, filter value]
         * we need to restore it back to
         * [tag, token, num?, primary key, filter value]
         */
        int[] fieldPermutation = new int[NUM_TAG_FIELDS + numTokenKeyPairFields + numFilterFields];
        int numSecondaryKeys = getNumSecondaryKeys();
        int idx = 0;
        // set tag fields
        for (int i = 0; i < NUM_TAG_FIELDS; i++) {
            fieldPermutation[idx++] = i + numSecondaryKeys;
        }
        // set secondary keys
        for (int i = 0; i < numSecondaryKeys; i++) {
            fieldPermutation[idx++] = i;
        }
        // set primary key + filter
        for (int i = 0; i < numPrimaryKeys + numFilterFields; i++) {
            fieldPermutation[idx++] = i + NUM_TAG_FIELDS + numSecondaryKeys;
        }
        return fieldPermutation;
    }

}
