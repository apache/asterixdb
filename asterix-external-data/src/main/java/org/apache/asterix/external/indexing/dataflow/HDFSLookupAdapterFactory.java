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
package edu.uci.ics.asterix.external.indexing.dataflow;

import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.common.dataflow.IAsterixApplicationContextInfo;
import edu.uci.ics.asterix.common.ioopcallbacks.LSMBTreeIOOperationCallbackFactory;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.ExternalDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.metadata.external.ExternalFileIndexAccessor;
import edu.uci.ics.asterix.metadata.external.ExternalLoopkupOperatorDiscriptor;
import edu.uci.ics.asterix.metadata.external.IControlledAdapter;
import edu.uci.ics.asterix.metadata.external.IControlledAdapterFactory;
import edu.uci.ics.asterix.metadata.external.IndexingConstants;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.metadata.utils.ExternalDatasetsRegistry;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.transaction.management.opcallbacks.SecondaryIndexOperationTrackerProvider;
import edu.uci.ics.asterix.transaction.management.opcallbacks.SecondaryIndexSearchOperationCallbackFactory;
import edu.uci.ics.asterix.transaction.management.service.transaction.AsterixRuntimeComponentsProvider;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;

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
