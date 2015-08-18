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
package edu.uci.ics.asterix.file;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.uci.ics.asterix.common.api.ILocalResourceMetadata;
import edu.uci.ics.asterix.common.config.AsterixStorageProperties;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.config.DatasetConfig.ExternalDatasetTransactionState;
import edu.uci.ics.asterix.common.config.DatasetConfig.ExternalFilePendingOp;
import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.common.config.IAsterixPropertiesProvider;
import edu.uci.ics.asterix.common.context.AsterixVirtualBufferCacheProvider;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.ioopcallbacks.LSMBTreeIOOperationCallbackFactory;
import edu.uci.ics.asterix.common.ioopcallbacks.LSMBTreeWithBuddyIOOperationCallbackFactory;
import edu.uci.ics.asterix.common.ioopcallbacks.LSMRTreeIOOperationCallbackFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.valueproviders.AqlPrimitiveValueProviderFactory;
import edu.uci.ics.asterix.external.adapter.factory.HDFSAdapterFactory;
import edu.uci.ics.asterix.external.adapter.factory.HDFSIndexingAdapterFactory;
import edu.uci.ics.asterix.external.adapter.factory.HiveAdapterFactory;
import edu.uci.ics.asterix.external.indexing.operators.ExternalDatasetIndexesAbortOperatorDescriptor;
import edu.uci.ics.asterix.external.indexing.operators.ExternalDatasetIndexesCommitOperatorDescriptor;
import edu.uci.ics.asterix.external.indexing.operators.ExternalDatasetIndexesRecoverOperatorDescriptor;
import edu.uci.ics.asterix.external.indexing.operators.IndexInfoOperatorDescriptor;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlTypeTraitProvider;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.ExternalDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.ExternalFile;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.metadata.external.FilesIndexDescription;
import edu.uci.ics.asterix.metadata.external.IndexingConstants;
import edu.uci.ics.asterix.metadata.feeds.ExternalDataScanOperatorDescriptor;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.metadata.utils.ExternalDatasetsRegistry;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.asterix.tools.external.data.ExternalFilesIndexOperatorDescriptor;
import edu.uci.ics.asterix.transaction.management.opcallbacks.SecondaryIndexOperationTrackerProvider;
import edu.uci.ics.asterix.transaction.management.resource.ExternalBTreeLocalResourceMetadata;
import edu.uci.ics.asterix.transaction.management.resource.PersistentLocalResourceFactoryProvider;
import edu.uci.ics.asterix.transaction.management.service.transaction.AsterixRuntimeComponentsProvider;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledCreateIndexStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledIndexDropStatement;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.ConnectorPolicyAssignmentPolicy;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeWithBuddyDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.dataflow.LSMTreeIndexCompactOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.dataflow.ExternalRTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreePolicyType;
import edu.uci.ics.hyracks.storage.common.file.LocalResource;

public class ExternalIndexingOperations {

    public static final List<List<String>> FILE_INDEX_FIELD_NAMES = new ArrayList<List<String>>();
    public static final ArrayList<IAType> FILE_INDEX_FIELD_TYPES = new ArrayList<IAType>();
    static {
        FILE_INDEX_FIELD_NAMES.add(new ArrayList<String>(Arrays.asList("")));
        FILE_INDEX_FIELD_TYPES.add(BuiltinType.ASTRING);
    }

    public static boolean isIndexible(ExternalDatasetDetails ds) {
        String adapter = ds.getAdapter();
        if (adapter.equalsIgnoreCase("hdfs") || adapter.equalsIgnoreCase("hive")
                || adapter.equalsIgnoreCase("edu.uci.ics.asterix.external.dataset.adapter.HDFSAdapter")
                || adapter.equalsIgnoreCase("edu.uci.ics.asterix.external.dataset.adapter.HIVEAdapter")) {
            return true;
        }
        return false;
    }

    public static boolean isRefereshActive(ExternalDatasetDetails ds) {
        return ds.getState() != ExternalDatasetTransactionState.COMMIT;
    }

    public static boolean datasetUsesHiveAdapter(ExternalDatasetDetails ds) {
        String adapter = ds.getAdapter();
        return (adapter.equalsIgnoreCase("hive") || adapter
                .equalsIgnoreCase("edu.uci.ics.asterix.external.dataset.adapter.HIVEAdapter"));
    }

    public static boolean isValidIndexName(String datasetName, String indexName) {
        return (!datasetName.concat(IndexingConstants.EXTERNAL_FILE_INDEX_NAME_SUFFIX).equals(indexName));
    }

    public static String getFilesIndexName(String datasetName) {
        return datasetName.concat(IndexingConstants.EXTERNAL_FILE_INDEX_NAME_SUFFIX);
    }

    public static int getRIDSize(Dataset dataset) {
        ExternalDatasetDetails dsd = ((ExternalDatasetDetails) dataset.getDatasetDetails());
        return IndexingConstants.getRIDSize(dsd.getProperties().get(IndexingConstants.KEY_INPUT_FORMAT));
    }

    public static IBinaryComparatorFactory[] getComparatorFactories(Dataset dataset) {
        ExternalDatasetDetails dsd = ((ExternalDatasetDetails) dataset.getDatasetDetails());
        return IndexingConstants.getComparatorFactories((dsd.getProperties().get(IndexingConstants.KEY_INPUT_FORMAT)));
    }

    public static IBinaryComparatorFactory[] getBuddyBtreeComparatorFactories() {
        return IndexingConstants.getBuddyBtreeComparatorFactories();
    }

    public static ArrayList<ExternalFile> getSnapshotFromExternalFileSystem(Dataset dataset) throws AlgebricksException {
        ArrayList<ExternalFile> files = new ArrayList<ExternalFile>();
        ExternalDatasetDetails datasetDetails = (ExternalDatasetDetails) dataset.getDatasetDetails();
        try {
            // Create the file system object
            FileSystem fs = getFileSystemObject(datasetDetails.getProperties());
            // If dataset uses hive adapter, add path to the dataset properties
            if (datasetUsesHiveAdapter(datasetDetails)) {
                HiveAdapterFactory.populateConfiguration(datasetDetails.getProperties());
            }
            // Get paths of dataset
            String path = datasetDetails.getProperties().get(HDFSAdapterFactory.KEY_PATH);
            String[] paths = path.split(",");

            // Add fileStatuses to files
            for (String aPath : paths) {
                FileStatus[] fileStatuses = fs.listStatus(new Path(aPath));
                for (int i = 0; i < fileStatuses.length; i++) {
                    int nextFileNumber = files.size();
                    if (fileStatuses[i].isDirectory()) {
                        listSubFiles(dataset, fs, fileStatuses[i], files);
                    } else {
                        files.add(new ExternalFile(dataset.getDataverseName(), dataset.getDatasetName(),
                                nextFileNumber, fileStatuses[i].getPath().toUri().getPath(), new Date(fileStatuses[i]
                                        .getModificationTime()), fileStatuses[i].getLen(),
                                ExternalFilePendingOp.PENDING_NO_OP));
                    }
                }
            }
            // Close file system
            fs.close();
            if (files.size() == 0) {
                throw new AlgebricksException("File Snapshot retrieved from external file system is empty");
            }
            return files;
        } catch (Exception e) {
            e.printStackTrace();
            throw new AlgebricksException("Unable to get list of HDFS files " + e);
        }
    }

    /* list all files under the directory
     * src is expected to be a folder
     */
    private static void listSubFiles(Dataset dataset, FileSystem srcFs, FileStatus src, ArrayList<ExternalFile> files)
            throws IOException {
        Path path = src.getPath();
        FileStatus[] fileStatuses = srcFs.listStatus(path);
        for (int i = 0; i < fileStatuses.length; i++) {
            int nextFileNumber = files.size();
            if (fileStatuses[i].isDirectory()) {
                listSubFiles(dataset, srcFs, fileStatuses[i], files);
            } else {
                files.add(new ExternalFile(dataset.getDataverseName(), dataset.getDatasetName(), nextFileNumber,
                        fileStatuses[i].getPath().toUri().getPath(), new Date(fileStatuses[i].getModificationTime()),
                        fileStatuses[i].getLen(), ExternalFilePendingOp.PENDING_NO_OP));
            }
        }
    }

    public static FileSystem getFileSystemObject(Map<String, String> map) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", map.get(HDFSAdapterFactory.KEY_HDFS_URL).trim());
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        return FileSystem.get(conf);
    }

    public static JobSpecification buildFilesIndexReplicationJobSpec(Dataset dataset,
            ArrayList<ExternalFile> externalFilesSnapshot, AqlMetadataProvider metadataProvider, boolean createIndex)
            throws MetadataException, AlgebricksException {
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        IAsterixPropertiesProvider asterixPropertiesProvider = AsterixAppContextInfo.getInstance();
        AsterixStorageProperties storageProperties = asterixPropertiesProvider.getStorageProperties();
        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(dataset,
                metadataProvider.getMetadataTxnContext());
        ILSMMergePolicyFactory mergePolicyFactory = compactionInfo.first;
        Map<String, String> mergePolicyFactoryProperties = compactionInfo.second;
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> secondarySplitsAndConstraint = metadataProvider
                .splitProviderAndPartitionConstraintsForFilesIndex(dataset.getDataverseName(),
                        dataset.getDatasetName(), getFilesIndexName(dataset.getDatasetName()), true);
        IFileSplitProvider secondaryFileSplitProvider = secondarySplitsAndConstraint.first;
        ILocalResourceMetadata localResourceMetadata = new ExternalBTreeLocalResourceMetadata(
                FilesIndexDescription.EXTERNAL_FILE_INDEX_TYPE_TRAITS,
                FilesIndexDescription.FILES_INDEX_COMP_FACTORIES, new int[] { 0 }, false, dataset.getDatasetId(),
                mergePolicyFactory, mergePolicyFactoryProperties);
        PersistentLocalResourceFactoryProvider localResourceFactoryProvider = new PersistentLocalResourceFactoryProvider(
                localResourceMetadata, LocalResource.ExternalBTreeResource);
        ExternalBTreeDataflowHelperFactory indexDataflowHelperFactory = new ExternalBTreeDataflowHelperFactory(
                mergePolicyFactory, mergePolicyFactoryProperties, new SecondaryIndexOperationTrackerProvider(
                        dataset.getDatasetId()), AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                LSMBTreeIOOperationCallbackFactory.INSTANCE, storageProperties.getBloomFilterFalsePositiveRate(),
                ExternalDatasetsRegistry.INSTANCE.getDatasetVersion(dataset), true);
        ExternalFilesIndexOperatorDescriptor externalFilesOp = new ExternalFilesIndexOperatorDescriptor(spec,
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                secondaryFileSplitProvider, indexDataflowHelperFactory, localResourceFactoryProvider,
                externalFilesSnapshot, createIndex);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, externalFilesOp,
                secondarySplitsAndConstraint.second);
        spec.addRoot(externalFilesOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }

    /**
     * This method create an indexing operator that index records in HDFS
     * 
     * @param jobSpec
     * @param itemType
     * @param dataset
     * @param format
     * @param files
     * @param indexerDesc
     * @return
     * @throws Exception
     */
    private static Pair<ExternalDataScanOperatorDescriptor, AlgebricksPartitionConstraint> getExternalDataIndexingOperator(
            JobSpecification jobSpec, IAType itemType, Dataset dataset, List<ExternalFile> files,
            RecordDescriptor indexerDesc, AqlMetadataProvider metadataProvider) throws Exception {
        HDFSIndexingAdapterFactory adapterFactory = new HDFSIndexingAdapterFactory();
        adapterFactory.setFiles(files);
        adapterFactory.configure(((ExternalDatasetDetails) dataset.getDatasetDetails()).getProperties(),
                (ARecordType) itemType);
        return new Pair<ExternalDataScanOperatorDescriptor, AlgebricksPartitionConstraint>(
                new ExternalDataScanOperatorDescriptor(jobSpec, indexerDesc, adapterFactory),
                adapterFactory.getPartitionConstraint());
    }

    public static Pair<ExternalDataScanOperatorDescriptor, AlgebricksPartitionConstraint> createExternalIndexingOp(
            JobSpecification spec, AqlMetadataProvider metadataProvider, Dataset dataset, ARecordType itemType,
            RecordDescriptor indexerDesc, List<ExternalFile> files) throws Exception {
        if (files == null) {
            files = MetadataManager.INSTANCE.getDatasetExternalFiles(metadataProvider.getMetadataTxnContext(), dataset);
        }
        return getExternalDataIndexingOperator(spec, itemType, dataset, files, indexerDesc, metadataProvider);
    }

    /**
     * At the end of this method, we expect to have 4 sets as follows:
     * metadataFiles should contain only the files that are appended in their original state
     * addedFiles should contain new files that has number assigned starting after the max original file number
     * deleteedFiles should contain files that are no longer there in the file system
     * appendedFiles should have the new file information of existing files
     * The method should return false in case of zero delta
     * 
     * @param dataset
     * @param metadataFiles
     * @param addedFiles
     * @param deletedFiles
     * @param appendedFiles
     * @return
     * @throws MetadataException
     * @throws AlgebricksException
     */
    public static boolean isDatasetUptodate(Dataset dataset, List<ExternalFile> metadataFiles,
            List<ExternalFile> addedFiles, List<ExternalFile> deletedFiles, List<ExternalFile> appendedFiles)
            throws MetadataException, AlgebricksException {
        boolean uptodate = true;
        int newFileNumber = metadataFiles.get(metadataFiles.size() - 1).getFileNumber() + 1;

        ArrayList<ExternalFile> fileSystemFiles = getSnapshotFromExternalFileSystem(dataset);

        // Loop over file system files < taking care of added files >
        for (ExternalFile fileSystemFile : fileSystemFiles) {
            boolean fileFound = false;
            Iterator<ExternalFile> mdFilesIterator = metadataFiles.iterator();
            while (mdFilesIterator.hasNext()) {
                ExternalFile metadataFile = mdFilesIterator.next();
                if (fileSystemFile.getFileName().equals(metadataFile.getFileName())) {
                    // Same file name
                    if (fileSystemFile.getLastModefiedTime().equals(metadataFile.getLastModefiedTime())) {
                        // Same timestamp
                        if (fileSystemFile.getSize() == metadataFile.getSize()) {
                            // Same size -> no op
                            mdFilesIterator.remove();
                            fileFound = true;
                        } else {
                            // Different size -> append op
                            metadataFile.setPendingOp(ExternalFilePendingOp.PENDING_APPEND_OP);
                            fileSystemFile.setPendingOp(ExternalFilePendingOp.PENDING_APPEND_OP);
                            appendedFiles.add(fileSystemFile);
                            fileFound = true;
                            uptodate = false;
                        }
                    } else {
                        // Same file name, Different file mod date -> delete and add
                        metadataFile.setPendingOp(ExternalFilePendingOp.PENDING_DROP_OP);
                        deletedFiles.add(new ExternalFile(metadataFile.getDataverseName(), metadataFile
                                .getDatasetName(), 0, metadataFile.getFileName(), metadataFile.getLastModefiedTime(),
                                metadataFile.getSize(), ExternalFilePendingOp.PENDING_DROP_OP));
                        fileSystemFile.setPendingOp(ExternalFilePendingOp.PENDING_ADD_OP);
                        fileSystemFile.setFileNumber(newFileNumber);
                        addedFiles.add(fileSystemFile);
                        newFileNumber++;
                        fileFound = true;
                        uptodate = false;
                    }
                }
                if (fileFound)
                    break;
            }
            if (!fileFound) {
                // File not stored previously in metadata -> pending add op
                fileSystemFile.setPendingOp(ExternalFilePendingOp.PENDING_ADD_OP);
                fileSystemFile.setFileNumber(newFileNumber);
                addedFiles.add(fileSystemFile);
                newFileNumber++;
                uptodate = false;
            }
        }

        // Done with files from external file system -> metadata files now contain both deleted files and appended ones
        // first, correct number assignment to deleted and updated files
        for (ExternalFile deletedFile : deletedFiles) {
            deletedFile.setFileNumber(newFileNumber);
            newFileNumber++;
        }
        for (ExternalFile appendedFile : appendedFiles) {
            appendedFile.setFileNumber(newFileNumber);
            newFileNumber++;
        }

        // include the remaining deleted files
        Iterator<ExternalFile> mdFilesIterator = metadataFiles.iterator();
        while (mdFilesIterator.hasNext()) {
            ExternalFile metadataFile = mdFilesIterator.next();
            if (metadataFile.getPendingOp() == ExternalFilePendingOp.PENDING_NO_OP) {
                metadataFile.setPendingOp(ExternalFilePendingOp.PENDING_DROP_OP);
                deletedFiles.add(new ExternalFile(metadataFile.getDataverseName(), metadataFile.getDatasetName(),
                        newFileNumber, metadataFile.getFileName(), metadataFile.getLastModefiedTime(), metadataFile
                                .getSize(), metadataFile.getPendingOp()));
                newFileNumber++;
                uptodate = false;
            }
        }
        return uptodate;
    }

    public static Dataset createTransactionDataset(Dataset dataset) {
        ExternalDatasetDetails originalDsd = (ExternalDatasetDetails) dataset.getDatasetDetails();
        ExternalDatasetDetails dsd = new ExternalDatasetDetails(originalDsd.getAdapter(), originalDsd.getProperties(),
                originalDsd.getTimestamp(), ExternalDatasetTransactionState.BEGIN);
        Dataset transactionDatset = new Dataset(dataset.getDataverseName(), dataset.getDatasetName(),
                dataset.getItemTypeName(), dataset.getNodeGroupName(), dataset.getCompactionPolicy(),
                dataset.getCompactionPolicyProperties(), dsd, dataset.getHints(), DatasetType.EXTERNAL,
                dataset.getDatasetId(), dataset.getPendingOp());
        return transactionDatset;
    }

    public static boolean isFileIndex(Index index) {
        return (index.getIndexName().equals(getFilesIndexName(index.getDatasetName())));
    }

    public static JobSpecification buildDropFilesIndexJobSpec(CompiledIndexDropStatement indexDropStmt,
            AqlMetadataProvider metadataProvider, Dataset dataset) throws AlgebricksException, MetadataException {
        String dataverseName = indexDropStmt.getDataverseName() == null ? metadataProvider.getDefaultDataverseName()
                : indexDropStmt.getDataverseName();
        String datasetName = indexDropStmt.getDatasetName();
        String indexName = indexDropStmt.getIndexName();
        boolean temp = dataset.getDatasetDetails().isTemp();
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = metadataProvider
                .splitProviderAndPartitionConstraintsForFilesIndex(dataverseName, datasetName, indexName, true);
        AsterixStorageProperties storageProperties = AsterixAppContextInfo.getInstance().getStorageProperties();
        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(dataset,
                metadataProvider.getMetadataTxnContext());
        IndexDropOperatorDescriptor btreeDrop = new IndexDropOperatorDescriptor(spec,
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                splitsAndConstraint.first, new LSMBTreeDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(
                        dataset.getDatasetId()), compactionInfo.first, compactionInfo.second,
                        new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                        AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMBTreeIOOperationCallbackFactory.INSTANCE,
                        storageProperties.getBloomFilterFalsePositiveRate(), false, null, null, null, null, !temp));
        AlgebricksPartitionConstraintHelper
                .setPartitionConstraintInJobSpec(spec, btreeDrop, splitsAndConstraint.second);
        spec.addRoot(btreeDrop);

        return spec;
    }

    public static JobSpecification buildFilesIndexUpdateOp(Dataset ds, List<ExternalFile> metadataFiles,
            List<ExternalFile> deletedFiles, List<ExternalFile> addedFiles, List<ExternalFile> appendedFiles,
            AqlMetadataProvider metadataProvider) throws MetadataException, AlgebricksException {
        ArrayList<ExternalFile> files = new ArrayList<ExternalFile>();
        for (ExternalFile file : metadataFiles) {
            if (file.getPendingOp() == ExternalFilePendingOp.PENDING_DROP_OP)
                files.add(file);
            else if (file.getPendingOp() == ExternalFilePendingOp.PENDING_APPEND_OP) {
                for (ExternalFile appendedFile : appendedFiles) {
                    if (appendedFile.getFileName().equals(file.getFileName())) {
                        files.add(new ExternalFile(file.getDataverseName(), file.getDatasetName(),
                                file.getFileNumber(), file.getFileName(), file.getLastModefiedTime(), appendedFile
                                        .getSize(), ExternalFilePendingOp.PENDING_NO_OP));
                    }
                }
            }
        }
        for (ExternalFile file : addedFiles) {
            files.add(file);
        }
        Collections.sort(files);
        return buildFilesIndexReplicationJobSpec(ds, files, metadataProvider, false);
    }

    public static JobSpecification buildIndexUpdateOp(Dataset ds, Index index, List<ExternalFile> metadataFiles,
            List<ExternalFile> deletedFiles, List<ExternalFile> addedFiles, List<ExternalFile> appendedFiles,
            AqlMetadataProvider metadataProvider) throws AsterixException, AlgebricksException {
        // Create files list
        ArrayList<ExternalFile> files = new ArrayList<ExternalFile>();

        for (ExternalFile metadataFile : metadataFiles) {
            if (metadataFile.getPendingOp() != ExternalFilePendingOp.PENDING_APPEND_OP) {
                files.add(metadataFile);
            } else {
                metadataFile.setPendingOp(ExternalFilePendingOp.PENDING_NO_OP);
                files.add(metadataFile);
            }
        }
        // add new files
        for (ExternalFile file : addedFiles) {
            files.add(file);
        }
        // add appended files
        for (ExternalFile file : appendedFiles) {
            files.add(file);
        }

        CompiledCreateIndexStatement ccis = new CompiledCreateIndexStatement(index.getIndexName(),
                index.getDataverseName(), index.getDatasetName(), index.getKeyFieldNames(), index.getKeyFieldTypes(),
                index.isEnforcingKeyFileds(), index.getGramLength(), index.getIndexType());
        return IndexOperations.buildSecondaryIndexLoadingJobSpec(ccis, null, null, metadataProvider, files);
    }

    public static JobSpecification buildCommitJob(Dataset ds, List<Index> indexes, AqlMetadataProvider metadataProvider)
            throws AlgebricksException, AsterixException {
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        IAsterixPropertiesProvider asterixPropertiesProvider = AsterixAppContextInfo.getInstance();
        AsterixStorageProperties storageProperties = asterixPropertiesProvider.getStorageProperties();
        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(ds,
                metadataProvider.getMetadataTxnContext());
        boolean temp = ds.getDatasetDetails().isTemp();
        ILSMMergePolicyFactory mergePolicyFactory = compactionInfo.first;
        Map<String, String> mergePolicyFactoryProperties = compactionInfo.second;
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> filesIndexSplitsAndConstraint = metadataProvider
                .splitProviderAndPartitionConstraintsForDataset(ds.getDataverseName(), ds.getDatasetName(),
                        getFilesIndexName(ds.getDatasetName()), temp);
        IFileSplitProvider filesIndexSplitProvider = filesIndexSplitsAndConstraint.first;
        ExternalBTreeDataflowHelperFactory filesIndexDataflowHelperFactory = getFilesIndexDataflowHelperFactory(ds,
                mergePolicyFactory, mergePolicyFactoryProperties, storageProperties, spec);
        IndexInfoOperatorDescriptor filesIndexInfo = new IndexInfoOperatorDescriptor(filesIndexSplitProvider,
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER);

        ArrayList<ExternalBTreeWithBuddyDataflowHelperFactory> btreeDataflowHelperFactories = new ArrayList<ExternalBTreeWithBuddyDataflowHelperFactory>();
        ArrayList<IndexInfoOperatorDescriptor> btreeInfos = new ArrayList<IndexInfoOperatorDescriptor>();
        ArrayList<ExternalRTreeDataflowHelperFactory> rtreeDataflowHelperFactories = new ArrayList<ExternalRTreeDataflowHelperFactory>();
        ArrayList<IndexInfoOperatorDescriptor> rtreeInfos = new ArrayList<IndexInfoOperatorDescriptor>();

        for (Index index : indexes) {
            if (isValidIndexName(index.getDatasetName(), index.getIndexName())) {
                Pair<IFileSplitProvider, AlgebricksPartitionConstraint> indexSplitsAndConstraint = metadataProvider
                        .splitProviderAndPartitionConstraintsForDataset(ds.getDataverseName(), ds.getDatasetName(),
                                index.getIndexName(), temp);
                if (index.getIndexType() == IndexType.BTREE) {
                    btreeDataflowHelperFactories.add(getBTreeDataflowHelperFactory(ds, index, mergePolicyFactory,
                            mergePolicyFactoryProperties, storageProperties, spec));
                    btreeInfos.add(new IndexInfoOperatorDescriptor(indexSplitsAndConstraint.first,
                            AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER));
                } else if (index.getIndexType() == IndexType.RTREE) {
                    rtreeDataflowHelperFactories.add(getRTreeDataflowHelperFactory(ds, index, mergePolicyFactory,
                            mergePolicyFactoryProperties, storageProperties, metadataProvider, spec));
                    rtreeInfos.add(new IndexInfoOperatorDescriptor(indexSplitsAndConstraint.first,
                            AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER));
                }
            }
        }

        ExternalDatasetIndexesCommitOperatorDescriptor op = new ExternalDatasetIndexesCommitOperatorDescriptor(spec,
                filesIndexDataflowHelperFactory, filesIndexInfo, btreeDataflowHelperFactories, btreeInfos,
                rtreeDataflowHelperFactories, rtreeInfos);

        spec.addRoot(op);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, op,
                filesIndexSplitsAndConstraint.second);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }

    private static ExternalBTreeDataflowHelperFactory getFilesIndexDataflowHelperFactory(Dataset ds,
            ILSMMergePolicyFactory mergePolicyFactory, Map<String, String> mergePolicyFactoryProperties,
            AsterixStorageProperties storageProperties, JobSpecification spec) {
        return new ExternalBTreeDataflowHelperFactory(mergePolicyFactory, mergePolicyFactoryProperties,
                new SecondaryIndexOperationTrackerProvider(ds.getDatasetId()),
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMBTreeIOOperationCallbackFactory.INSTANCE,
                storageProperties.getBloomFilterFalsePositiveRate(),
                ExternalDatasetsRegistry.INSTANCE.getDatasetVersion(ds), true);
    }

    private static ExternalBTreeWithBuddyDataflowHelperFactory getBTreeDataflowHelperFactory(Dataset ds, Index index,
            ILSMMergePolicyFactory mergePolicyFactory, Map<String, String> mergePolicyFactoryProperties,
            AsterixStorageProperties storageProperties, JobSpecification spec) {
        return new ExternalBTreeWithBuddyDataflowHelperFactory(mergePolicyFactory, mergePolicyFactoryProperties,
                new SecondaryIndexOperationTrackerProvider(ds.getDatasetId()),
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                LSMBTreeWithBuddyIOOperationCallbackFactory.INSTANCE,
                storageProperties.getBloomFilterFalsePositiveRate(), new int[] { index.getKeyFieldNames().size() },
                ExternalDatasetsRegistry.INSTANCE.getDatasetVersion(ds), true);
    }

    @SuppressWarnings("rawtypes")
    private static ExternalRTreeDataflowHelperFactory getRTreeDataflowHelperFactory(Dataset ds, Index index,
            ILSMMergePolicyFactory mergePolicyFactory, Map<String, String> mergePolicyFactoryProperties,
            AsterixStorageProperties storageProperties, AqlMetadataProvider metadataProvider, JobSpecification spec)
            throws AlgebricksException, AsterixException {
        int numPrimaryKeys = getRIDSize(ds);
        List<List<String>> secondaryKeyFields = index.getKeyFieldNames();
        secondaryKeyFields.size();
        ARecordType itemType = (ARecordType) metadataProvider.findType(ds.getDataverseName(), ds.getItemTypeName());
        Pair<IAType, Boolean> spatialTypePair = Index.getNonNullableKeyFieldType(secondaryKeyFields.get(0), itemType);
        IAType spatialType = spatialTypePair.first;
        if (spatialType == null) {
            throw new AsterixException("Could not find field " + secondaryKeyFields.get(0) + " in the schema.");
        }
        int numDimensions = NonTaggedFormatUtil.getNumDimensions(spatialType.getTypeTag());
        int numNestedSecondaryKeyFields = numDimensions * 2;
        IPrimitiveValueProviderFactory[] valueProviderFactories = new IPrimitiveValueProviderFactory[numNestedSecondaryKeyFields];
        IBinaryComparatorFactory[] secondaryComparatorFactories = new IBinaryComparatorFactory[numNestedSecondaryKeyFields];

        ISerializerDeserializer[] secondaryRecFields = new ISerializerDeserializer[numPrimaryKeys
                + numNestedSecondaryKeyFields];
        ITypeTraits[] secondaryTypeTraits = new ITypeTraits[numNestedSecondaryKeyFields + numPrimaryKeys];
        IAType nestedKeyType = NonTaggedFormatUtil.getNestedSpatialType(spatialType.getTypeTag());
        ATypeTag keyType = nestedKeyType.getTypeTag();

        keyType = nestedKeyType.getTypeTag();
        for (int i = 0; i < numNestedSecondaryKeyFields; i++) {
            ISerializerDeserializer keySerde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(nestedKeyType);
            secondaryRecFields[i] = keySerde;

            secondaryComparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(
                    nestedKeyType, true);
            secondaryTypeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(nestedKeyType);
            valueProviderFactories[i] = AqlPrimitiveValueProviderFactory.INSTANCE;
        }
        // Add serializers and comparators for primary index fields.
        for (int i = 0; i < numPrimaryKeys; i++) {
            secondaryRecFields[numNestedSecondaryKeyFields + i] = IndexingConstants.getSerializerDeserializer(i);
            secondaryTypeTraits[numNestedSecondaryKeyFields + i] = IndexingConstants.getTypeTraits(i);
        }
        int[] primaryKeyFields = new int[numPrimaryKeys];
        for (int i = 0; i < primaryKeyFields.length; i++) {
            primaryKeyFields[i] = i + numNestedSecondaryKeyFields;
        }

        return new ExternalRTreeDataflowHelperFactory(valueProviderFactories, RTreePolicyType.RTREE,
                getBuddyBtreeComparatorFactories(), mergePolicyFactory, mergePolicyFactoryProperties,
                new SecondaryIndexOperationTrackerProvider(ds.getDatasetId()),
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMRTreeIOOperationCallbackFactory.INSTANCE,
                AqlMetadataProvider.proposeLinearizer(keyType, secondaryComparatorFactories.length),
                storageProperties.getBloomFilterFalsePositiveRate(), new int[] { index.getKeyFieldNames().size() },
                ExternalDatasetsRegistry.INSTANCE.getDatasetVersion(ds), true);
    }

    public static JobSpecification buildAbortOp(Dataset ds, List<Index> indexes, AqlMetadataProvider metadataProvider)
            throws AlgebricksException, AsterixException {
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        IAsterixPropertiesProvider asterixPropertiesProvider = AsterixAppContextInfo.getInstance();
        AsterixStorageProperties storageProperties = asterixPropertiesProvider.getStorageProperties();
        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(ds,
                metadataProvider.getMetadataTxnContext());
        ILSMMergePolicyFactory mergePolicyFactory = compactionInfo.first;
        Map<String, String> mergePolicyFactoryProperties = compactionInfo.second;

        boolean temp = ds.getDatasetDetails().isTemp();
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> filesIndexSplitsAndConstraint = metadataProvider
                .splitProviderAndPartitionConstraintsForDataset(ds.getDataverseName(), ds.getDatasetName(),
                        getFilesIndexName(ds.getDatasetName()), temp);
        IFileSplitProvider filesIndexSplitProvider = filesIndexSplitsAndConstraint.first;
        ExternalBTreeDataflowHelperFactory filesIndexDataflowHelperFactory = getFilesIndexDataflowHelperFactory(ds,
                mergePolicyFactory, mergePolicyFactoryProperties, storageProperties, spec);
        IndexInfoOperatorDescriptor filesIndexInfo = new IndexInfoOperatorDescriptor(filesIndexSplitProvider,
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER);

        ArrayList<ExternalBTreeWithBuddyDataflowHelperFactory> btreeDataflowHelperFactories = new ArrayList<ExternalBTreeWithBuddyDataflowHelperFactory>();
        ArrayList<IndexInfoOperatorDescriptor> btreeInfos = new ArrayList<IndexInfoOperatorDescriptor>();
        ArrayList<ExternalRTreeDataflowHelperFactory> rtreeDataflowHelperFactories = new ArrayList<ExternalRTreeDataflowHelperFactory>();
        ArrayList<IndexInfoOperatorDescriptor> rtreeInfos = new ArrayList<IndexInfoOperatorDescriptor>();

        for (Index index : indexes) {
            if (isValidIndexName(index.getDatasetName(), index.getIndexName())) {
                Pair<IFileSplitProvider, AlgebricksPartitionConstraint> indexSplitsAndConstraint = metadataProvider
                        .splitProviderAndPartitionConstraintsForDataset(ds.getDataverseName(), ds.getDatasetName(),
                                index.getIndexName(), temp);
                if (index.getIndexType() == IndexType.BTREE) {
                    btreeDataflowHelperFactories.add(getBTreeDataflowHelperFactory(ds, index, mergePolicyFactory,
                            mergePolicyFactoryProperties, storageProperties, spec));
                    btreeInfos.add(new IndexInfoOperatorDescriptor(indexSplitsAndConstraint.first,
                            AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER));
                } else if (index.getIndexType() == IndexType.RTREE) {
                    rtreeDataflowHelperFactories.add(getRTreeDataflowHelperFactory(ds, index, mergePolicyFactory,
                            mergePolicyFactoryProperties, storageProperties, metadataProvider, spec));
                    rtreeInfos.add(new IndexInfoOperatorDescriptor(indexSplitsAndConstraint.first,
                            AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER));
                }
            }
        }

        ExternalDatasetIndexesAbortOperatorDescriptor op = new ExternalDatasetIndexesAbortOperatorDescriptor(spec,
                filesIndexDataflowHelperFactory, filesIndexInfo, btreeDataflowHelperFactories, btreeInfos,
                rtreeDataflowHelperFactories, rtreeInfos);

        spec.addRoot(op);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, op,
                filesIndexSplitsAndConstraint.second);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;

    }

    public static JobSpecification buildRecoverOp(Dataset ds, List<Index> indexes, AqlMetadataProvider metadataProvider)
            throws AlgebricksException, AsterixException {
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        IAsterixPropertiesProvider asterixPropertiesProvider = AsterixAppContextInfo.getInstance();
        AsterixStorageProperties storageProperties = asterixPropertiesProvider.getStorageProperties();
        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(ds,
                metadataProvider.getMetadataTxnContext());
        ILSMMergePolicyFactory mergePolicyFactory = compactionInfo.first;
        Map<String, String> mergePolicyFactoryProperties = compactionInfo.second;
        boolean temp = ds.getDatasetDetails().isTemp();

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> filesIndexSplitsAndConstraint = metadataProvider
                .splitProviderAndPartitionConstraintsForDataset(ds.getDataverseName(), ds.getDatasetName(),
                        getFilesIndexName(ds.getDatasetName()), temp);
        IFileSplitProvider filesIndexSplitProvider = filesIndexSplitsAndConstraint.first;
        ExternalBTreeDataflowHelperFactory filesIndexDataflowHelperFactory = getFilesIndexDataflowHelperFactory(ds,
                mergePolicyFactory, mergePolicyFactoryProperties, storageProperties, spec);
        IndexInfoOperatorDescriptor filesIndexInfo = new IndexInfoOperatorDescriptor(filesIndexSplitProvider,
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER);

        ArrayList<ExternalBTreeWithBuddyDataflowHelperFactory> btreeDataflowHelperFactories = new ArrayList<ExternalBTreeWithBuddyDataflowHelperFactory>();
        ArrayList<IndexInfoOperatorDescriptor> btreeInfos = new ArrayList<IndexInfoOperatorDescriptor>();
        ArrayList<ExternalRTreeDataflowHelperFactory> rtreeDataflowHelperFactories = new ArrayList<ExternalRTreeDataflowHelperFactory>();
        ArrayList<IndexInfoOperatorDescriptor> rtreeInfos = new ArrayList<IndexInfoOperatorDescriptor>();

        for (Index index : indexes) {
            if (isValidIndexName(index.getDatasetName(), index.getIndexName())) {
                Pair<IFileSplitProvider, AlgebricksPartitionConstraint> indexSplitsAndConstraint = metadataProvider
                        .splitProviderAndPartitionConstraintsForDataset(ds.getDataverseName(), ds.getDatasetName(),
                                index.getIndexName(), temp);
                if (index.getIndexType() == IndexType.BTREE) {
                    btreeDataflowHelperFactories.add(getBTreeDataflowHelperFactory(ds, index, mergePolicyFactory,
                            mergePolicyFactoryProperties, storageProperties, spec));
                    btreeInfos.add(new IndexInfoOperatorDescriptor(indexSplitsAndConstraint.first,
                            AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER));
                } else if (index.getIndexType() == IndexType.RTREE) {
                    rtreeDataflowHelperFactories.add(getRTreeDataflowHelperFactory(ds, index, mergePolicyFactory,
                            mergePolicyFactoryProperties, storageProperties, metadataProvider, spec));
                    rtreeInfos.add(new IndexInfoOperatorDescriptor(indexSplitsAndConstraint.first,
                            AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER));
                }
            }
        }

        ExternalDatasetIndexesRecoverOperatorDescriptor op = new ExternalDatasetIndexesRecoverOperatorDescriptor(spec,
                filesIndexDataflowHelperFactory, filesIndexInfo, btreeDataflowHelperFactories, btreeInfos,
                rtreeDataflowHelperFactories, rtreeInfos);

        spec.addRoot(op);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, op,
                filesIndexSplitsAndConstraint.second);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }

    public static JobSpecification compactFilesIndexJobSpec(Dataset dataset, AqlMetadataProvider metadataProvider)
            throws MetadataException, AlgebricksException {
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        IAsterixPropertiesProvider asterixPropertiesProvider = AsterixAppContextInfo.getInstance();
        AsterixStorageProperties storageProperties = asterixPropertiesProvider.getStorageProperties();
        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(dataset,
                metadataProvider.getMetadataTxnContext());
        ILSMMergePolicyFactory mergePolicyFactory = compactionInfo.first;
        Map<String, String> mergePolicyFactoryProperties = compactionInfo.second;
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> secondarySplitsAndConstraint = metadataProvider
                .splitProviderAndPartitionConstraintsForFilesIndex(dataset.getDataverseName(),
                        dataset.getDatasetName(), getFilesIndexName(dataset.getDatasetName()), true);
        IFileSplitProvider secondaryFileSplitProvider = secondarySplitsAndConstraint.first;
        ExternalBTreeDataflowHelperFactory indexDataflowHelperFactory = new ExternalBTreeDataflowHelperFactory(
                mergePolicyFactory, mergePolicyFactoryProperties, new SecondaryIndexOperationTrackerProvider(
                        dataset.getDatasetId()), AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                LSMBTreeIOOperationCallbackFactory.INSTANCE, storageProperties.getBloomFilterFalsePositiveRate(),
                ExternalDatasetsRegistry.INSTANCE.getDatasetVersion(dataset), true);
        LSMTreeIndexCompactOperatorDescriptor compactOp = new LSMTreeIndexCompactOperatorDescriptor(spec,
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                secondaryFileSplitProvider, FilesIndexDescription.EXTERNAL_FILE_INDEX_TYPE_TRAITS,
                FilesIndexDescription.FILES_INDEX_COMP_FACTORIES, new int[] { 0 }, indexDataflowHelperFactory,
                NoOpOperationCallbackFactory.INSTANCE);
        spec.addRoot(compactOp);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, compactOp,
                secondarySplitsAndConstraint.second);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }
}
