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
package org.apache.asterix.external.indexing.dataflow;

import java.util.List;
import java.util.Map;

import org.apache.asterix.common.dataflow.IAsterixApplicationContextInfo;
import org.apache.asterix.common.ioopcallbacks.LSMBTreeIOOperationCallbackFactory;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.ExternalDatasetDetails;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.external.ExternalFileIndexAccessor;
import org.apache.asterix.metadata.external.ExternalLoopkupOperatorDiscriptor;
import org.apache.asterix.metadata.external.IControlledAdapter;
import org.apache.asterix.metadata.external.IControlledAdapterFactory;
import org.apache.asterix.metadata.external.IndexingConstants;
import org.apache.asterix.metadata.utils.DatasetUtils;
import org.apache.asterix.metadata.utils.ExternalDatasetsRegistry;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.transaction.management.opcallbacks.SecondaryIndexOperationTrackerProvider;
import org.apache.asterix.transaction.management.opcallbacks.SecondaryIndexSearchOperationCallbackFactory;
import org.apache.asterix.transaction.management.service.transaction.AsterixRuntimeComponentsProvider;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;

// This class takes care of creating the adapter based on the formats and input format
public class HDFSLookupAdapterFactory implements IControlledAdapterFactory {

    private static final long serialVersionUID = 1L;

    private Map<String, String> adapterConfiguration;
    private IAType atype;
    private boolean propagateInput;
    private int[] ridFields;
    private int[] propagatedFields;
    private boolean retainNull;

    @Override
    public void configure(IAType atype, boolean propagateInput, int[] ridFields,
            Map<String, String> adapterConfiguration, boolean retainNull) {
        this.adapterConfiguration = adapterConfiguration;
        this.atype = atype;
        this.propagateInput = propagateInput;
        this.ridFields = ridFields;
        this.retainNull = retainNull;
    }

    @Override
    public IControlledAdapter createAdapter(IHyracksTaskContext ctx, ExternalFileIndexAccessor fileIndexAccessor,
            RecordDescriptor inRecDesc) {
        if (propagateInput) {
            configurePropagatedFields(inRecDesc);
        }
        return new HDFSLookupAdapter(atype, inRecDesc, adapterConfiguration, propagateInput, ridFields,
                propagatedFields, ctx, fileIndexAccessor, retainNull);
    }

    private void configurePropagatedFields(RecordDescriptor inRecDesc) {
        int ptr = 0;
        boolean skip = false;
        propagatedFields = new int[inRecDesc.getFieldCount() - ridFields.length];
        for (int i = 0; i < inRecDesc.getFieldCount(); i++) {
            if (ptr < ridFields.length) {
                skip = false;
                for (int j = 0; j < ridFields.length; j++) {
                    if (ridFields[j] == i) {
                        ptr++;
                        skip = true;
                        break;
                    }
                }
                if (!skip)
                    propagatedFields[i - ptr] = i;
            } else {
                propagatedFields[i - ptr] = i;
            }
        }
    }

    /*
     * This function creates an operator that uses the built indexes in asterix to perform record lookup over external data
     */
    public static Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildExternalDataLookupRuntime(
            JobSpecification jobSpec, Dataset dataset, Index secondaryIndex, int[] ridIndexes, boolean retainInput,
            IVariableTypeEnvironment typeEnv, List<LogicalVariable> outputVars, IOperatorSchema opSchema,
            JobGenContext context, AqlMetadataProvider metadataProvider, boolean retainNull) throws AlgebricksException {

        // Get data type
        IAType itemType = null;
        try {
            itemType = MetadataManager.INSTANCE.getDatatype(metadataProvider.getMetadataTxnContext(),
                    dataset.getDataverseName(), dataset.getItemTypeName()).getDatatype();
        } catch (MetadataException e) {
            e.printStackTrace();
            throw new AlgebricksException("Unable to get item type from metadata " + e);
        }
        if (itemType.getTypeTag() != ATypeTag.RECORD) {
            throw new AlgebricksException("Can only scan datasets of records.");
        }

        // Create the adapter factory <- right now there is only one. if there are more in the future, we can create a map->
        ExternalDatasetDetails datasetDetails = (ExternalDatasetDetails) dataset.getDatasetDetails();
        HDFSLookupAdapterFactory adapterFactory = new HDFSLookupAdapterFactory();
        adapterFactory.configure(itemType, retainInput, ridIndexes, datasetDetails.getProperties(), retainNull);

        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo;
        try {
            compactionInfo = DatasetUtils.getMergePolicyFactory(dataset, metadataProvider.getMetadataTxnContext());
        } catch (MetadataException e) {
            throw new AlgebricksException(" Unabel to create merge policy factory for external dataset", e);
        }

        boolean temp = dataset.getDatasetDetails().isTemp();
        // Create the file index data flow helper
        ExternalBTreeDataflowHelperFactory indexDataflowHelperFactory = new ExternalBTreeDataflowHelperFactory(
                compactionInfo.first, compactionInfo.second, new SecondaryIndexOperationTrackerProvider(
                        dataset.getDatasetId()), AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                LSMBTreeIOOperationCallbackFactory.INSTANCE, metadataProvider.getStorageProperties()
                        .getBloomFilterFalsePositiveRate(), ExternalDatasetsRegistry.INSTANCE.getAndLockDatasetVersion(
                        dataset, metadataProvider), !temp);

        // Create the out record descriptor, appContext and fileSplitProvider for the files index
        RecordDescriptor outRecDesc = JobGenHelper.mkRecordDescriptor(typeEnv, opSchema, context);
        IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> spPc;
        try {
            spPc = metadataProvider.splitProviderAndPartitionConstraintsForFilesIndex(dataset.getDataverseName(),
                    dataset.getDatasetName(),
                    dataset.getDatasetName().concat(IndexingConstants.EXTERNAL_FILE_INDEX_NAME_SUFFIX), false);
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }

        ISearchOperationCallbackFactory searchOpCallbackFactory = temp ? NoOpOperationCallbackFactory.INSTANCE
                : new SecondaryIndexSearchOperationCallbackFactory();
        // Create the operator
        ExternalLoopkupOperatorDiscriptor op = new ExternalLoopkupOperatorDiscriptor(jobSpec, adapterFactory,
                outRecDesc, indexDataflowHelperFactory, retainInput, appContext.getIndexLifecycleManagerProvider(),
                appContext.getStorageManagerInterface(), spPc.first, dataset.getDatasetId(), metadataProvider
                        .getStorageProperties().getBloomFilterFalsePositiveRate(), searchOpCallbackFactory, retainNull,
                context.getNullWriterFactory());

        // Return value
        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(op, spPc.second);
    }
}
