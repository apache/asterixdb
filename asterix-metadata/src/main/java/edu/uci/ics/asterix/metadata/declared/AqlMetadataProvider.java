/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.asterix.metadata.declared;

import java.io.File;
import java.util.List;

import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.dataflow.IAsterixApplicationContextInfo;
import edu.uci.ics.asterix.common.parse.IParseFileSplitsDecl;
import edu.uci.ics.asterix.dataflow.data.nontagged.valueproviders.AqlPrimitiveValueProviderFactory;
import edu.uci.ics.asterix.external.data.adapter.api.IDatasourceAdapter;
import edu.uci.ics.asterix.external.data.adapter.api.IDatasourceReadAdapter;
import edu.uci.ics.asterix.external.data.operator.ExternalDataScanOperatorDescriptor;
import edu.uci.ics.asterix.feed.comm.IFeedMessage;
import edu.uci.ics.asterix.feed.mgmt.FeedId;
import edu.uci.ics.asterix.feed.operator.FeedIntakeOperatorDescriptor;
import edu.uci.ics.asterix.feed.operator.FeedMessageOperatorDescriptor;
import edu.uci.ics.asterix.formats.base.IDataFormat;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlTypeTraitProvider;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl.IndexKind;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.asterix.runtime.transaction.TreeIndexInsertUpdateDeleteOperatorDescriptor;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSink;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSource;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.std.SinkWriterRuntimeFactory;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.rtree.dataflow.RTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.rtree.dataflow.RTreeSearchOperatorDescriptor;

public class AqlMetadataProvider implements
		IMetadataProvider<AqlSourceId, String> {
	private final long txnId;
	private boolean isWriteTransaction;
	private final AqlCompiledMetadataDeclarations metadata;

	public AqlMetadataProvider(long txnId, boolean isWriteTransaction,
			AqlCompiledMetadataDeclarations metadata) {
		this.txnId = txnId;
		this.isWriteTransaction = isWriteTransaction;
		this.metadata = metadata;
	}

	@Override
	public AqlDataSource findDataSource(AqlSourceId id)
			throws AlgebricksException {
		AqlSourceId aqlId = (AqlSourceId) id;
		return lookupSourceInMetadata(metadata, aqlId);
	}

	public AqlCompiledMetadataDeclarations getMetadataDeclarations() {
		return metadata;
	}

	public boolean isWriteTransaction() {
		return isWriteTransaction;
	}

	@Override
	public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getScannerRuntime(
			IDataSource<AqlSourceId> dataSource,
			List<LogicalVariable> scanVariables,
			List<LogicalVariable> projectVariables, boolean projectPushed,
			JobGenContext context, JobSpecification jobSpec)
			throws AlgebricksException {
		AqlCompiledDatasetDecl adecl = metadata.findDataset(dataSource.getId()
				.getDatasetName());
		if (adecl == null) {
			throw new AlgebricksException("Unknown dataset "
					+ dataSource.getId().getDatasetName());
		}
		switch (adecl.getDatasetType()) {
		case FEED:
			if (dataSource instanceof ExternalFeedDataSource) {
				return buildExternalDatasetScan(jobSpec, adecl, dataSource);
			} else {
				return buildInternalDatasetScan(jobSpec, adecl, dataSource,
						context);
			}

		case INTERNAL: {
			return buildInternalDatasetScan(jobSpec, adecl, dataSource, context);
		}

		case EXTERNAL: {
			return buildExternalDatasetScan(jobSpec, adecl, dataSource);
		}

		default: {
			throw new IllegalArgumentException();
		}
		}
	}

	private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildInternalDatasetScan(
			JobSpecification jobSpec, AqlCompiledDatasetDecl acedl,
			IDataSource<AqlSourceId> dataSource, JobGenContext context)
			throws AlgebricksException {
		AqlSourceId asid = dataSource.getId();
		String datasetName = asid.getDatasetName();
		String indexName = DatasetUtils.getPrimaryIndex(acedl).getIndexName();

		try {
			return buildBtreeRuntime(metadata, context, jobSpec, datasetName,
					acedl, indexName, null, null, true, true);
		} catch (AlgebricksException e) {
			throw new AlgebricksException(e);
		}
	}

	private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildExternalDatasetScan(
			JobSpecification jobSpec, AqlCompiledDatasetDecl acedl,
			IDataSource<AqlSourceId> dataSource) throws AlgebricksException {
		String itemTypeName = acedl.getItemTypeName();
		IAType itemType;
		try {
			itemType = metadata.findType(itemTypeName);
		} catch (Exception e) {
			throw new AlgebricksException(e);
		}

		if (dataSource instanceof ExternalFeedDataSource) {
			AqlCompiledFeedDatasetDetails acfdd = (AqlCompiledFeedDatasetDetails) ((ExternalFeedDataSource) dataSource)
					.getCompiledDatasetDecl().getAqlCompiledDatasetDetails();

			return buildFeedIntakeRuntime(jobSpec, metadata.getDataverseName(),
					acedl.getName(), itemType, acfdd, metadata.getFormat());
		} else {
			return buildExternalDataScannerRuntime(jobSpec, itemType,
					(AqlCompiledExternalDatasetDetails) acedl
							.getAqlCompiledDatasetDetails(), metadata
							.getFormat());
		}
	}

	@SuppressWarnings("rawtypes")
	public static Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildExternalDataScannerRuntime(
			JobSpecification jobSpec, IAType itemType,
			AqlCompiledExternalDatasetDetails decl, IDataFormat format)
			throws AlgebricksException {
		if (itemType.getTypeTag() != ATypeTag.RECORD) {
			throw new AlgebricksException("Can only scan datasets of records.");
		}

		IDatasourceReadAdapter adapter;
		try {
			adapter = (IDatasourceReadAdapter) Class.forName(decl.getAdapter())
					.newInstance();
		} catch (Exception e) {
			e.printStackTrace();
			throw new AlgebricksException("unable to load the adapter class "
					+ e);
		}

		if (!(adapter.getAdapterType().equals(
				IDatasourceAdapter.AdapterType.READ) || adapter
				.getAdapterType().equals(
						IDatasourceAdapter.AdapterType.READ_WRITE))) {
			throw new AlgebricksException(
					"external dataset adapter does not support read operation");
		}
		ARecordType rt = (ARecordType) itemType;

		try {
			adapter.configure(decl.getProperties(), itemType);
		} catch (Exception e) {
			e.printStackTrace();
			throw new AlgebricksException(
					"unable to configure the datasource adapter " + e);
		}

		ISerializerDeserializer payloadSerde = format.getSerdeProvider()
				.getSerializerDeserializer(itemType);
		RecordDescriptor scannerDesc = new RecordDescriptor(
				new ISerializerDeserializer[] { payloadSerde });

		ExternalDataScanOperatorDescriptor dataScanner = new ExternalDataScanOperatorDescriptor(
				jobSpec, decl.getAdapter(), decl.getProperties(), rt,
				scannerDesc);
		dataScanner.setDatasourceAdapter(adapter);
		AlgebricksPartitionConstraint constraint = adapter
				.getPartitionConstraint();
		return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(
				dataScanner, constraint);
	}

	@SuppressWarnings("rawtypes")
	public static Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildScannerRuntime(
			JobSpecification jobSpec, IAType itemType,
			IParseFileSplitsDecl decl, IDataFormat format)
			throws AlgebricksException {
		if (itemType.getTypeTag() != ATypeTag.RECORD) {
			throw new AlgebricksException("Can only scan datasets of records.");
		}
		ARecordType rt = (ARecordType) itemType;
		ITupleParserFactory tupleParser = format.createTupleParser(rt, decl);
		FileSplit[] splits = decl.getSplits();
		IFileSplitProvider scannerSplitProvider = new ConstantFileSplitProvider(
				splits);
		ISerializerDeserializer payloadSerde = format.getSerdeProvider()
				.getSerializerDeserializer(itemType);
		RecordDescriptor scannerDesc = new RecordDescriptor(
				new ISerializerDeserializer[] { payloadSerde });
		IOperatorDescriptor scanner = new FileScanOperatorDescriptor(jobSpec,
				scannerSplitProvider, tupleParser, scannerDesc);
		String[] locs = new String[splits.length];
		for (int i = 0; i < splits.length; i++) {
			locs[i] = splits[i].getNodeName();
		}
		AlgebricksPartitionConstraint apc = new AlgebricksAbsolutePartitionConstraint(
				locs);
		return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(
				scanner, apc);
	}

	@SuppressWarnings("rawtypes")
	public static Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildFeedIntakeRuntime(
			JobSpecification jobSpec, String dataverse, String dataset,
			IAType itemType, AqlCompiledFeedDatasetDetails decl,
			IDataFormat format) throws AlgebricksException {
		if (itemType.getTypeTag() != ATypeTag.RECORD) {
			throw new AlgebricksException("Can only consume records.");
		}
		IDatasourceAdapter adapter;
		try {
			adapter = (IDatasourceAdapter) Class.forName(decl.getAdapter())
					.newInstance();
		} catch (Exception e) {
			e.printStackTrace();
			throw new AlgebricksException("unable to load the adapter class "
					+ e);
		}

		ARecordType rt = (ARecordType) itemType;
		try {
			adapter.configure(decl.getProperties(), itemType);
		} catch (Exception e) {
			e.printStackTrace();
			throw new AlgebricksException(
					"unable to configure the datasource adapter " + e);
		}

		ISerializerDeserializer payloadSerde = format.getSerdeProvider()
				.getSerializerDeserializer(itemType);
		RecordDescriptor feedDesc = new RecordDescriptor(
				new ISerializerDeserializer[] { payloadSerde });

		FeedIntakeOperatorDescriptor feedIngestor = new FeedIntakeOperatorDescriptor(
				jobSpec, new FeedId(dataverse, dataset), decl.getAdapter(),
				decl.getProperties(), rt, feedDesc);

		AlgebricksPartitionConstraint constraint = adapter
				.getPartitionConstraint();
		return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(
				feedIngestor, constraint);
	}

	@SuppressWarnings("rawtypes")
	public static Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildFeedMessengerRuntime(
			JobSpecification jobSpec, AqlCompiledMetadataDeclarations metadata,
			AqlCompiledFeedDatasetDetails decl, String dataverse,
			String dataset, List<IFeedMessage> feedMessages)
			throws AlgebricksException {

		Pair<IFileSplitProvider, AlgebricksPartitionConstraint> spPc;
		try {
			spPc = metadata
					.splitProviderAndPartitionConstraintsForInternalOrFeedDataset(
							dataset, dataset);
		} catch (Exception e) {
			throw new AlgebricksException(e);
		}

		FeedMessageOperatorDescriptor feedMessenger = new FeedMessageOperatorDescriptor(
				jobSpec, dataverse, dataset, feedMessages);

		return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(
				feedMessenger, spPc.second);
	}

	@SuppressWarnings("rawtypes")
	public static Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildBtreeRuntime(
			AqlCompiledMetadataDeclarations metadata, JobGenContext context,
			JobSpecification jobSpec, String datasetName,
			AqlCompiledDatasetDecl ddecl, String indexName, int[] lowKeyFields,
			int[] highKeyFields, boolean lowKeyInclusive,
			boolean highKeyInclusive) throws AlgebricksException {
		String itemTypeName = ddecl.getItemTypeName();
		IAType itemType;
		try {
			itemType = metadata.findType(itemTypeName);
		} catch (Exception e) {
			throw new AlgebricksException(e);
		}

		boolean isSecondary = true;
		AqlCompiledIndexDecl primIdxDecl = DatasetUtils.getPrimaryIndex(ddecl);

		if (primIdxDecl != null) {
			isSecondary = !indexName.equals(primIdxDecl.getIndexName());
		}

		int numPrimaryKeys = DatasetUtils.getPartitioningFunctions(ddecl)
				.size();
		ISerializerDeserializer[] recordFields;
		IBinaryComparatorFactory[] comparatorFactories;
		ITypeTraits[] typeTraits;
		int numSecondaryKeys = 0;
		int i = 0;
		if (isSecondary) {
			AqlCompiledIndexDecl cid = DatasetUtils.findSecondaryIndexByName(
					ddecl, indexName);
			if (cid == null) {
				throw new AlgebricksException(
						"Code generation error: no index " + indexName
								+ " for dataset " + datasetName);
			}
			List<String> secondaryKeyFields = cid.getFieldExprs();
			numSecondaryKeys = secondaryKeyFields.size();
			int numKeys = numSecondaryKeys + numPrimaryKeys;
			recordFields = new ISerializerDeserializer[numKeys];
			typeTraits = new ITypeTraits[numKeys];
			// comparatorFactories = new
			// IBinaryComparatorFactory[numSecondaryKeys];
			comparatorFactories = new IBinaryComparatorFactory[numKeys];
			if (itemType.getTypeTag() != ATypeTag.RECORD) {
				throw new AlgebricksException(
						"Only record types can be indexed.");
			}
			ARecordType recType = (ARecordType) itemType;
			for (i = 0; i < numSecondaryKeys; i++) {
				IAType keyType = AqlCompiledIndexDecl.keyFieldType(
						secondaryKeyFields.get(i), recType);
				ISerializerDeserializer keySerde = metadata.getFormat()
						.getSerdeProvider().getSerializerDeserializer(keyType);
				recordFields[i] = keySerde;
				comparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE
						.getBinaryComparatorFactory(keyType, true);
				typeTraits[i] = AqlTypeTraitProvider.INSTANCE
						.getTypeTrait(keyType);
			}
		} else {
			recordFields = new ISerializerDeserializer[numPrimaryKeys + 1];
			comparatorFactories = new IBinaryComparatorFactory[numPrimaryKeys];
			typeTraits = new ITypeTraits[numPrimaryKeys + 1];
			ISerializerDeserializer payloadSerde = metadata.getFormat()
					.getSerdeProvider().getSerializerDeserializer(itemType);
			recordFields[numPrimaryKeys] = payloadSerde;
			typeTraits[numPrimaryKeys] = AqlTypeTraitProvider.INSTANCE
					.getTypeTrait(itemType);
		}

		for (Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> evalFactoryAndType : DatasetUtils
				.getPartitioningFunctions(ddecl)) {
			IAType keyType = evalFactoryAndType.third;
			ISerializerDeserializer keySerde = metadata.getFormat()
					.getSerdeProvider().getSerializerDeserializer(keyType);
			recordFields[i] = keySerde;
			// if (!isSecondary) {
			comparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE
					.getBinaryComparatorFactory(keyType, true);
			// }
			typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
			++i;
		}

		IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context
				.getAppContext();
		RecordDescriptor recDesc = new RecordDescriptor(recordFields);

		Pair<IFileSplitProvider, AlgebricksPartitionConstraint> spPc;
		try {
			spPc = metadata
					.splitProviderAndPartitionConstraintsForInternalOrFeedDataset(
							datasetName, indexName);
		} catch (Exception e) {
			throw new AlgebricksException(e);
		}

		BTreeSearchOperatorDescriptor btreeSearchOp = new BTreeSearchOperatorDescriptor(
				jobSpec, recDesc, appContext.getStorageManagerInterface(),
				appContext.getIndexRegistryProvider(), spPc.first,
				typeTraits,
				comparatorFactories, lowKeyFields, highKeyFields,
				lowKeyInclusive, highKeyInclusive,
				new BTreeDataflowHelperFactory(), NoOpOperationCallbackProvider.INSTANCE);
		return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(
				btreeSearchOp, spPc.second);
	}

	@SuppressWarnings("rawtypes")
	public static Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildRtreeRuntime(
			AqlCompiledMetadataDeclarations metadata, JobGenContext context,
			JobSpecification jobSpec, String datasetName,
			AqlCompiledDatasetDecl ddecl, String indexName, int[] keyFields)
			throws AlgebricksException {
		String itemTypeName = ddecl.getItemTypeName();
		IAType itemType;
		try {
			itemType = metadata.findType(itemTypeName);
		} catch (Exception e) {
			throw new AlgebricksException(e);
		}

		boolean isSecondary = true;
		AqlCompiledIndexDecl primIdxDecl = DatasetUtils.getPrimaryIndex(ddecl);
		if (primIdxDecl != null) {
			isSecondary = !indexName.equals(primIdxDecl.getIndexName());
		}

		int numPrimaryKeys = DatasetUtils.getPartitioningFunctions(ddecl)
				.size();
		ISerializerDeserializer[] recordFields;
		IBinaryComparatorFactory[] comparatorFactories;
		ITypeTraits[] typeTraits;
		IPrimitiveValueProviderFactory[] valueProviderFactories;
		int numSecondaryKeys = 0;
		int numNestedSecondaryKeyFields = 0;
		int i = 0;
		if (isSecondary) {
			AqlCompiledIndexDecl cid = DatasetUtils.findSecondaryIndexByName(
					ddecl, indexName);
			if (cid == null) {
				throw new AlgebricksException(
						"Code generation error: no index " + indexName
								+ " for dataset " + datasetName);
			}
			List<String> secondaryKeyFields = cid.getFieldExprs();
			numSecondaryKeys = secondaryKeyFields.size();

			if (numSecondaryKeys != 1) {
				throw new AlgebricksException(
						"Cannot use "
								+ numSecondaryKeys
								+ " fields as a key for the R-tree index. There can be only one field as a key for the R-tree index.");
			}

			if (itemType.getTypeTag() != ATypeTag.RECORD) {
				throw new AlgebricksException(
						"Only record types can be indexed.");
			}
			ARecordType recType = (ARecordType) itemType;

			IAType keyType = AqlCompiledIndexDecl.keyFieldType(
					secondaryKeyFields.get(0), recType);
			if (keyType == null) {
				throw new AlgebricksException("Could not find field "
						+ secondaryKeyFields.get(0) + " in the schema.");
			}

			int dimension = NonTaggedFormatUtil.getNumDimensions(keyType
					.getTypeTag());
			numNestedSecondaryKeyFields = dimension * 2;

			int numFields = numNestedSecondaryKeyFields + numPrimaryKeys;
			recordFields = new ISerializerDeserializer[numFields];
			typeTraits = new ITypeTraits[numFields];
			comparatorFactories = new IBinaryComparatorFactory[numNestedSecondaryKeyFields];
			valueProviderFactories = new IPrimitiveValueProviderFactory[numNestedSecondaryKeyFields];

			IAType nestedKeyType = NonTaggedFormatUtil
					.getNestedSpatialType(keyType.getTypeTag());
			for (i = 0; i < numNestedSecondaryKeyFields; i++) {
				ISerializerDeserializer keySerde = AqlSerializerDeserializerProvider.INSTANCE
						.getSerializerDeserializer(nestedKeyType);
				recordFields[i] = keySerde;
				comparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE
						.getBinaryComparatorFactory(nestedKeyType,
								true);
				typeTraits[i] = AqlTypeTraitProvider.INSTANCE
						.getTypeTrait(nestedKeyType);
				valueProviderFactories[i] = AqlPrimitiveValueProviderFactory.INSTANCE;
			}
		} else {
			throw new AlgebricksException(
					"R-tree can only be used as a secondary index");
		}

		for (Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> evalFactoryAndType : DatasetUtils
				.getPartitioningFunctions(ddecl)) {
			IAType keyType = evalFactoryAndType.third;
			ISerializerDeserializer keySerde = AqlSerializerDeserializerProvider.INSTANCE
					.getSerializerDeserializer(keyType);
			recordFields[i] = keySerde;
			typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
			++i;
		}

		IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context
				.getAppContext();
		RecordDescriptor recDesc = new RecordDescriptor(recordFields);

		Pair<IFileSplitProvider, AlgebricksPartitionConstraint> spPc;
		try {
			spPc = metadata
					.splitProviderAndPartitionConstraintsForInternalOrFeedDataset(
							datasetName, indexName);
		} catch (Exception e) {
			throw new AlgebricksException(e);
		}

		RTreeSearchOperatorDescriptor rtreeSearchOp = new RTreeSearchOperatorDescriptor(
				jobSpec, recDesc, appContext.getStorageManagerInterface(),
				appContext.getIndexRegistryProvider(), spPc.first,
				typeTraits,
				comparatorFactories, keyFields,
				new RTreeDataflowHelperFactory(valueProviderFactories), NoOpOperationCallbackProvider.INSTANCE);

		return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(
				rtreeSearchOp, spPc.second);
	}

	@Override
	public Pair<IPushRuntimeFactory, AlgebricksPartitionConstraint> getWriteFileRuntime(
			IDataSink sink, int[] printColumns,
			IPrinterFactory[] printerFactories, RecordDescriptor inputDesc) {
		FileSplitDataSink fsds = (FileSplitDataSink) sink;
		FileSplitSinkId fssi = (FileSplitSinkId) fsds.getId();
		FileSplit fs = fssi.getFileSplit();
		File outFile = fs.getLocalFile().getFile();
		String nodeId = fs.getNodeName();

		SinkWriterRuntimeFactory runtime = new SinkWriterRuntimeFactory(
				printColumns, printerFactories, outFile, metadata
						.getWriterFactory(), inputDesc);
		AlgebricksPartitionConstraint apc = new AlgebricksAbsolutePartitionConstraint(
				new String[] { nodeId });
		return new Pair<IPushRuntimeFactory, AlgebricksPartitionConstraint>(
				runtime, apc);
	}

	@Override
	public IDataSourceIndex<String, AqlSourceId> findDataSourceIndex(
			String indexId, AqlSourceId dataSourceId)
			throws AlgebricksException {
		AqlDataSource ads = findDataSource(dataSourceId);
		AqlCompiledDatasetDecl adecl = ads.getCompiledDatasetDecl();
		if (adecl.getDatasetType() == DatasetType.EXTERNAL) {
			throw new AlgebricksException("No index for external dataset "
					+ dataSourceId);
		}

		String idxName = (String) indexId;
		AqlCompiledIndexDecl acid = DatasetUtils.findSecondaryIndexByName(
				adecl, idxName);
		AqlSourceId asid = (AqlSourceId) dataSourceId;
		if (acid != null) {
			return new AqlIndex(acid, metadata, asid.getDatasetName());
		} else {
			AqlCompiledIndexDecl primIdx = DatasetUtils.getPrimaryIndex(adecl);
			if (primIdx.getIndexName().equals(indexId)) {
				return new AqlIndex(primIdx, metadata, asid.getDatasetName());
			} else {
				return null;
			}
		}
	}

	public static AqlDataSource lookupSourceInMetadata(
			AqlCompiledMetadataDeclarations metadata, AqlSourceId aqlId)
			throws AlgebricksException {
		if (!aqlId.getDataverseName().equals(metadata.getDataverseName())) {
			return null;
		}
		AqlCompiledDatasetDecl acdd = metadata.findDataset(aqlId
				.getDatasetName());
		if (acdd == null) {
			throw new AlgebricksException("Datasource with id " + aqlId
					+ " was not found.");
		}
		String tName = acdd.getItemTypeName();
		IAType itemType;
		try {
			itemType = metadata.findType(tName);
		} catch (Exception e) {
			throw new AlgebricksException(e);
		}
		return new AqlDataSource(aqlId, acdd, itemType);
	}

	@Override
	public boolean scannerOperatorIsLeaf(IDataSource<AqlSourceId> dataSource) {
		AqlSourceId asid = dataSource.getId();
		String datasetName = asid.getDatasetName();
		AqlCompiledDatasetDecl adecl = metadata.findDataset(datasetName);
		if (adecl == null) {
			throw new IllegalArgumentException("Unknown dataset " + datasetName);
		}
		return adecl.getDatasetType() == DatasetType.EXTERNAL;
	}

	@Override
	public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getWriteResultRuntime(
			IDataSource<AqlSourceId> dataSource,
			IOperatorSchema propagatedSchema, List<LogicalVariable> keys,
			LogicalVariable payload, JobGenContext context,
			JobSpecification spec) throws AlgebricksException {
		String datasetName = dataSource.getId().getDatasetName();
		int numKeys = keys.size();
		// move key fields to front
		int[] fieldPermutation = new int[numKeys + 1];
		// System.arraycopy(keys, 0, fieldPermutation, 0, numKeys);
		int i = 0;
		for (LogicalVariable varKey : keys) {
			int idx = propagatedSchema.findVariable(varKey);
			fieldPermutation[i] = idx;
			i++;
		}
		fieldPermutation[numKeys] = propagatedSchema.findVariable(payload);

		AqlCompiledDatasetDecl compiledDatasetDecl = metadata
				.findDataset(datasetName);
		if (compiledDatasetDecl == null) {
			throw new AlgebricksException("Unknown dataset " + datasetName);
		}
		String indexName = DatasetUtils.getPrimaryIndex(compiledDatasetDecl)
				.getIndexName();

		ITypeTraits[] typeTraits = DatasetUtils.computeTupleTypeTraits(
				compiledDatasetDecl, metadata);

		IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context
				.getAppContext();

		IBinaryComparatorFactory[] comparatorFactories = DatasetUtils
				.computeKeysBinaryComparatorFactories(compiledDatasetDecl,
						context.getBinaryComparatorFactoryProvider());

		Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint;
		try {
			splitsAndConstraint = metadata
					.splitProviderAndPartitionConstraintsForInternalOrFeedDataset(
							datasetName, indexName);
		} catch (Exception e) {
			throw new AlgebricksException(e);
		}

		TreeIndexBulkLoadOperatorDescriptor btreeBulkLoad = new TreeIndexBulkLoadOperatorDescriptor(
				spec, appContext.getStorageManagerInterface(),
				appContext.getIndexRegistryProvider(),
				splitsAndConstraint.first, typeTraits, comparatorFactories,
				fieldPermutation, GlobalConfig.DEFAULT_BTREE_FILL_FACTOR,
				new BTreeDataflowHelperFactory(),
				NoOpOperationCallbackProvider.INSTANCE);
		return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(
				btreeBulkLoad, splitsAndConstraint.second);
	}

	@Override
	public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getInsertRuntime(
			IDataSource<AqlSourceId> dataSource,
			IOperatorSchema propagatedSchema, List<LogicalVariable> keys,
			LogicalVariable payload, RecordDescriptor recordDesc,
			JobGenContext context, JobSpecification spec)
			throws AlgebricksException {
		String datasetName = dataSource.getId().getDatasetName();
		int numKeys = keys.size();
		// move key fields to front
		int[] fieldPermutation = new int[numKeys + 1];
		// System.arraycopy(keys, 0, fieldPermutation, 0, numKeys);
		int i = 0;
		for (LogicalVariable varKey : keys) {
			int idx = propagatedSchema.findVariable(varKey);
			fieldPermutation[i] = idx;
			i++;
		}
		fieldPermutation[numKeys] = propagatedSchema.findVariable(payload);

		AqlCompiledDatasetDecl compiledDatasetDecl = metadata
				.findDataset(datasetName);
		if (compiledDatasetDecl == null) {
			throw new AlgebricksException("Unknown dataset " + datasetName);
		}
		String indexName = DatasetUtils.getPrimaryIndex(compiledDatasetDecl)
				.getIndexName();

		ITypeTraits[] typeTraits = DatasetUtils.computeTupleTypeTraits(
				compiledDatasetDecl, metadata);

		IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context
				.getAppContext();

		IBinaryComparatorFactory[] comparatorFactories = DatasetUtils
				.computeKeysBinaryComparatorFactories(compiledDatasetDecl,
						context.getBinaryComparatorFactoryProvider());

		Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint;
		try {
			splitsAndConstraint = metadata
					.splitProviderAndPartitionConstraintsForInternalOrFeedDataset(
							datasetName, indexName);
		} catch (Exception e) {
			throw new AlgebricksException(e);
		}

		TreeIndexInsertUpdateDeleteOperatorDescriptor btreeBulkLoad = new TreeIndexInsertUpdateDeleteOperatorDescriptor(
				spec, recordDesc, appContext.getStorageManagerInterface(),
				appContext.getIndexRegistryProvider(),
				splitsAndConstraint.first, typeTraits, comparatorFactories,
				fieldPermutation, IndexOp.INSERT,
				new BTreeDataflowHelperFactory(),
				NoOpOperationCallbackProvider.INSTANCE, txnId);
		return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(
				btreeBulkLoad, splitsAndConstraint.second);
	}

	@Override
	public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getDeleteRuntime(
			IDataSource<AqlSourceId> dataSource,
			IOperatorSchema propagatedSchema, List<LogicalVariable> keys,
			LogicalVariable payload, RecordDescriptor recordDesc,
			JobGenContext context, JobSpecification spec)
			throws AlgebricksException {
		String datasetName = dataSource.getId().getDatasetName();
		int numKeys = keys.size();
		// move key fields to front
		int[] fieldPermutation = new int[numKeys + 1];
		// System.arraycopy(keys, 0, fieldPermutation, 0, numKeys);
		int i = 0;
		for (LogicalVariable varKey : keys) {
			int idx = propagatedSchema.findVariable(varKey);
			fieldPermutation[i] = idx;
			i++;
		}
		fieldPermutation[numKeys] = propagatedSchema.findVariable(payload);

		AqlCompiledDatasetDecl compiledDatasetDecl = metadata
				.findDataset(datasetName);
		if (compiledDatasetDecl == null) {
			throw new AlgebricksException("Unknown dataset " + datasetName);
		}
		String indexName = DatasetUtils.getPrimaryIndex(compiledDatasetDecl)
				.getIndexName();

		ITypeTraits[] typeTraits = DatasetUtils.computeTupleTypeTraits(
				compiledDatasetDecl, metadata);

		IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context
				.getAppContext();

		IBinaryComparatorFactory[] comparatorFactories = DatasetUtils
				.computeKeysBinaryComparatorFactories(compiledDatasetDecl,
						context.getBinaryComparatorFactoryProvider());

		Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint;
		try {
			splitsAndConstraint = metadata
					.splitProviderAndPartitionConstraintsForInternalOrFeedDataset(
							datasetName, indexName);
		} catch (Exception e) {
			throw new AlgebricksException(e);
		}

		TreeIndexInsertUpdateDeleteOperatorDescriptor btreeBulkLoad = new TreeIndexInsertUpdateDeleteOperatorDescriptor(
				spec, recordDesc, appContext.getStorageManagerInterface(),
				appContext.getIndexRegistryProvider(),
				splitsAndConstraint.first, typeTraits, comparatorFactories,
				fieldPermutation, IndexOp.DELETE,
				new BTreeDataflowHelperFactory(),
				NoOpOperationCallbackProvider.INSTANCE, txnId);
		return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(
				btreeBulkLoad, splitsAndConstraint.second);
	}

	@Override
	public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexInsertRuntime(
			IDataSourceIndex<String, AqlSourceId> dataSourceIndex,
			IOperatorSchema propagatedSchema,
			List<LogicalVariable> primaryKeys,
			List<LogicalVariable> secondaryKeys, RecordDescriptor recordDesc,
			JobGenContext context, JobSpecification spec)
			throws AlgebricksException {
		String indexName = dataSourceIndex.getId();
		String datasetName = dataSourceIndex.getDataSource().getId()
				.getDatasetName();
		AqlCompiledDatasetDecl compiledDatasetDecl = metadata
				.findDataset(datasetName);
		if (compiledDatasetDecl == null) {
			throw new AlgebricksException("Unknown dataset " + datasetName);
		}
		AqlCompiledIndexDecl cid = DatasetUtils.findSecondaryIndexByName(
				compiledDatasetDecl, indexName);

		if (cid.getKind() == IndexKind.BTREE)
			return getBTreeDmlRuntime(datasetName, indexName, propagatedSchema,
					primaryKeys, secondaryKeys, recordDesc, context, spec,
					IndexOp.INSERT);
		else
			return getRTreeDmlRuntime(datasetName, indexName, propagatedSchema,
					primaryKeys, secondaryKeys, recordDesc, context, spec,
					IndexOp.INSERT);
	}

	@Override
	public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexDeleteRuntime(
			IDataSourceIndex<String, AqlSourceId> dataSourceIndex,
			IOperatorSchema propagatedSchema,
			List<LogicalVariable> primaryKeys,
			List<LogicalVariable> secondaryKeys, RecordDescriptor recordDesc,
			JobGenContext context, JobSpecification spec)
			throws AlgebricksException {
		String indexName = dataSourceIndex.getId();
		String datasetName = dataSourceIndex.getDataSource().getId()
				.getDatasetName();
		AqlCompiledDatasetDecl compiledDatasetDecl = metadata
				.findDataset(datasetName);
		if (compiledDatasetDecl == null) {
			throw new AlgebricksException("Unknown dataset " + datasetName);
		}
		AqlCompiledIndexDecl cid = DatasetUtils.findSecondaryIndexByName(
				compiledDatasetDecl, indexName);
		if (cid.getKind() == IndexKind.BTREE)
			return getBTreeDmlRuntime(datasetName, indexName, propagatedSchema,
					primaryKeys, secondaryKeys, recordDesc, context, spec,
					IndexOp.DELETE);
		else
			return getRTreeDmlRuntime(datasetName, indexName, propagatedSchema,
					primaryKeys, secondaryKeys, recordDesc, context, spec,
					IndexOp.DELETE);
	}

	private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getBTreeDmlRuntime(
			String datasetName, String indexName,
			IOperatorSchema propagatedSchema,
			List<LogicalVariable> primaryKeys,
			List<LogicalVariable> secondaryKeys, RecordDescriptor recordDesc,
			JobGenContext context, JobSpecification spec, IndexOp indexOp)
			throws AlgebricksException {
		int numKeys = primaryKeys.size() + secondaryKeys.size();
		// generate field permutations
		int[] fieldPermutation = new int[numKeys];
		int i = 0;
		for (LogicalVariable varKey : secondaryKeys) {
			int idx = propagatedSchema.findVariable(varKey);
			fieldPermutation[i] = idx;
			i++;
		}
		for (LogicalVariable varKey : primaryKeys) {
			int idx = propagatedSchema.findVariable(varKey);
			fieldPermutation[i] = idx;
			i++;
		}

		// dataset
		AqlCompiledDatasetDecl compiledDatasetDecl = metadata
				.findDataset(datasetName);
		if (compiledDatasetDecl == null) {
			throw new AlgebricksException("Unknown dataset " + datasetName);
		}
		String itemTypeName = compiledDatasetDecl.getItemTypeName();
		IAType itemType;
		try {
			itemType = metadata.findType(itemTypeName);
		} catch (Exception e) {
			throw new AlgebricksException(e);
		}
		if (itemType.getTypeTag() != ATypeTag.RECORD) {
			throw new AlgebricksException("Only record types can be indexed.");
		}
		ARecordType recType = (ARecordType) itemType;

		// index parameters
		AqlCompiledIndexDecl cid = DatasetUtils.findSecondaryIndexByName(
				compiledDatasetDecl, indexName);
		List<String> secondaryKeyExprs = cid.getFieldExprs();
		ITypeTraits[] typeTraits = new ITypeTraits[numKeys];
		IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[numKeys];
		for (i = 0; i < secondaryKeys.size(); ++i) {
			IAType keyType = AqlCompiledIndexDecl.keyFieldType(
					secondaryKeyExprs.get(i).toString(), recType);
			comparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE
					.getBinaryComparatorFactory(keyType, true);
			typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
		}
		for (Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> evalFactoryAndType : DatasetUtils
				.getPartitioningFunctions(compiledDatasetDecl)) {
			IAType keyType = evalFactoryAndType.third;
			comparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE
					.getBinaryComparatorFactory(keyType, true);
			typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
			++i;
		}

		IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context
				.getAppContext();
		Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint;
		try {
			splitsAndConstraint = metadata
					.splitProviderAndPartitionConstraintsForInternalOrFeedDataset(
							datasetName, indexName);
		} catch (Exception e) {
			throw new AlgebricksException(e);
		}
		TreeIndexInsertUpdateDeleteOperatorDescriptor btreeBulkLoad = new TreeIndexInsertUpdateDeleteOperatorDescriptor(
				spec, recordDesc, appContext.getStorageManagerInterface(),
				appContext.getIndexRegistryProvider(),
				splitsAndConstraint.first, typeTraits, comparatorFactories,
				fieldPermutation, indexOp, new BTreeDataflowHelperFactory(),
				NoOpOperationCallbackProvider.INSTANCE, txnId);
		return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(
				btreeBulkLoad, splitsAndConstraint.second);
	}

	private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getRTreeDmlRuntime(
			String datasetName, String indexName,
			IOperatorSchema propagatedSchema,
			List<LogicalVariable> primaryKeys,
			List<LogicalVariable> secondaryKeys, RecordDescriptor recordDesc,
			JobGenContext context, JobSpecification spec, IndexOp indexOp)
			throws AlgebricksException {
		AqlCompiledDatasetDecl compiledDatasetDecl = metadata
				.findDataset(datasetName);
		String itemTypeName = compiledDatasetDecl.getItemTypeName();
		IAType itemType;
		try {
			itemType = metadata.findType(itemTypeName);
		} catch (Exception e) {
			throw new AlgebricksException(e);
		}
		if (itemType.getTypeTag() != ATypeTag.RECORD) {
			throw new AlgebricksException("Only record types can be indexed.");
		}
		ARecordType recType = (ARecordType) itemType;
		AqlCompiledIndexDecl cid = DatasetUtils.findSecondaryIndexByName(
				compiledDatasetDecl, indexName);
		List<String> secondaryKeyExprs = cid.getFieldExprs();
		IAType spatialType = AqlCompiledIndexDecl.keyFieldType(
				secondaryKeyExprs.get(0).toString(), recType);
		int dimension = NonTaggedFormatUtil.getNumDimensions(spatialType
				.getTypeTag());
		int numSecondaryKeys = dimension * 2;
		int numPrimaryKeys = primaryKeys.size();
		int numKeys = numSecondaryKeys + numPrimaryKeys;
		ITypeTraits[] typeTraits = new ITypeTraits[numKeys];
		IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[numKeys];
		int[] fieldPermutation = new int[numKeys];
		int i = 0;

		for (LogicalVariable varKey : secondaryKeys) {
			int idx = propagatedSchema.findVariable(varKey);
			fieldPermutation[i] = idx;
			i++;
		}
		for (LogicalVariable varKey : primaryKeys) {
			int idx = propagatedSchema.findVariable(varKey);
			fieldPermutation[i] = idx;
			i++;
		}
		IAType nestedKeyType = NonTaggedFormatUtil
				.getNestedSpatialType(spatialType.getTypeTag());
		IPrimitiveValueProviderFactory[] valueProviderFactories = new IPrimitiveValueProviderFactory[numSecondaryKeys];
		for (i = 0; i < numSecondaryKeys; i++) {
			ISerializerDeserializer keySerde = AqlSerializerDeserializerProvider.INSTANCE
					.getSerializerDeserializer(nestedKeyType);
			comparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE
					.getBinaryComparatorFactory(nestedKeyType, true);
			typeTraits[i] = AqlTypeTraitProvider.INSTANCE
					.getTypeTrait(nestedKeyType);
			valueProviderFactories[i] = AqlPrimitiveValueProviderFactory.INSTANCE;
		}
		for (Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> evalFactoryAndType : DatasetUtils
				.getPartitioningFunctions(compiledDatasetDecl)) {
			IAType keyType = evalFactoryAndType.third;
			comparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE
					.getBinaryComparatorFactory(keyType, true);
			typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
			++i;
		}

		IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context
				.getAppContext();
		Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint;
		try {
			splitsAndConstraint = metadata
					.splitProviderAndPartitionConstraintsForInternalOrFeedDataset(
							datasetName, indexName);
		} catch (Exception e) {
			throw new AlgebricksException(e);
		}
		TreeIndexInsertUpdateDeleteOperatorDescriptor rtreeUpdate = new TreeIndexInsertUpdateDeleteOperatorDescriptor(
				spec, recordDesc, appContext.getStorageManagerInterface(),
				appContext.getIndexRegistryProvider(),
				splitsAndConstraint.first, typeTraits, comparatorFactories,
				fieldPermutation, indexOp, new RTreeDataflowHelperFactory(
						valueProviderFactories),
				NoOpOperationCallbackProvider.INSTANCE, txnId);
		return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(
				rtreeUpdate, splitsAndConstraint.second);
	}

	public long getTxnId() {
		return txnId;
	}

	public static ITreeIndexFrameFactory createBTreeNSMInteriorFrameFactory(
			ITypeTraits[] typeTraits) {
		return new BTreeNSMInteriorFrameFactory(
				new TypeAwareTupleWriterFactory(typeTraits));
	}

	public static ITreeIndexFrameFactory createBTreeNSMLeafFrameFactory(
			ITypeTraits[] typeTraits) {
		return new BTreeNSMLeafFrameFactory(new TypeAwareTupleWriterFactory(
				typeTraits));
	}

	@Override
	public IFunctionInfo lookupFunction(FunctionIdentifier fid) {
		return AsterixBuiltinFunctions.lookupFunction(fid);
	}

}
