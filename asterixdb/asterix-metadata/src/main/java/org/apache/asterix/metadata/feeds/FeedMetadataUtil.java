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
package org.apache.asterix.metadata.feeds;

import java.rmi.RemoteException;
import java.util.Map;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.asterix.common.exceptions.NoOpWarningCollector;
import org.apache.asterix.common.external.IDataSourceAdapter;
import org.apache.asterix.common.external.IDataSourceAdapter.AdapterType;
import org.apache.asterix.common.functions.ExternalFunctionLanguage;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.external.adapter.factory.ExternalAdapterFactory;
import org.apache.asterix.external.api.ITypedAdapterFactory;
import org.apache.asterix.external.feed.api.IFeed;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.provider.AdapterFactoryProvider;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.DatasourceAdapter;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedPolicyEntity;
import org.apache.asterix.metadata.entities.Library;
import org.apache.asterix.metadata.utils.MetadataConstants;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;

/**
 * A utility class for providing helper functions for feeds TODO: Refactor this
 * class.
 */
public class FeedMetadataUtil {

    private FeedMetadataUtil() {
    }

    public static Dataset validateIfDatasetExists(MetadataProvider metadataProvider, DataverseName dataverseName,
            String datasetName) throws AlgebricksException {
        Dataset dataset = metadataProvider.findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new CompilationException("Unknown target dataset :" + datasetName);
        }

        if (!dataset.getDatasetType().equals(DatasetType.INTERNAL)) {
            throw new CompilationException("Statement not applicable. Dataset " + datasetName
                    + " is not of required type " + DatasetType.INTERNAL);
        }
        return dataset;
    }

    public static Feed validateIfFeedExists(DataverseName dataverseName, String feedName,
            MetadataTransactionContext ctx) throws AlgebricksException {
        Feed feed = MetadataManager.INSTANCE.getFeed(ctx, dataverseName, feedName);
        if (feed == null) {
            throw new CompilationException("Unknown source feed: " + feedName);
        }
        return feed;
    }

    public static FeedPolicyEntity validateIfPolicyExists(DataverseName dataverseName, String policyName,
            MetadataTransactionContext ctx) throws AlgebricksException {
        FeedPolicyEntity feedPolicy = MetadataManager.INSTANCE.getFeedPolicy(ctx, dataverseName, policyName);
        if (feedPolicy == null) {
            feedPolicy =
                    MetadataManager.INSTANCE.getFeedPolicy(ctx, MetadataConstants.METADATA_DATAVERSE_NAME, policyName);
            if (feedPolicy == null) {
                throw new CompilationException("Unknown feed policy" + policyName);
            }
        }
        return feedPolicy;
    }

    public static void validateFeed(Feed feed, MetadataTransactionContext mdTxnCtx, ICcApplicationContext appCtx,
            IWarningCollector warningCollector) throws AlgebricksException {
        try {
            Map<String, String> configuration = feed.getConfiguration();
            ARecordType adapterOutputType = getOutputType(feed, configuration.get(ExternalDataConstants.KEY_TYPE_NAME));
            ARecordType metaType = getOutputType(feed, configuration.get(ExternalDataConstants.KEY_META_TYPE_NAME));
            ExternalDataUtils.prepareFeed(configuration, feed.getDataverseName(), feed.getFeedName());
            // Get adapter from metadata dataset <Metadata dataverse>
            String adapterName = configuration.get(ExternalDataConstants.KEY_ADAPTER_NAME);
            if (adapterName == null) {
                throw new AlgebricksException("cannot find adapter name");
            }
            DatasourceAdapter adapterEntity = MetadataManager.INSTANCE.getAdapter(mdTxnCtx,
                    MetadataConstants.METADATA_DATAVERSE_NAME, adapterName);
            // Get adapter from metadata dataset <The feed dataverse>
            if (adapterEntity == null) {
                adapterEntity = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, feed.getDataverseName(), adapterName);
            }
            AdapterType adapterType;
            ITypedAdapterFactory adapterFactory;
            if (adapterEntity != null) {
                adapterType = adapterEntity.getType();
                String adapterFactoryClassname = adapterEntity.getClassname();
                switch (adapterType) {
                    case INTERNAL:
                        adapterFactory = (ITypedAdapterFactory) Class.forName(adapterFactoryClassname).newInstance();
                        break;
                    case EXTERNAL:
                        adapterFactory = createExternalAdapterFactory(mdTxnCtx, adapterEntity, adapterFactoryClassname);
                        break;
                    default:
                        throw new AsterixException("Unknown Adapter type " + adapterType);
                }
            } else {
                ExternalDataUtils.prepare(adapterName, configuration);
                adapterFactory = (ITypedAdapterFactory) appCtx.getAdapterFactoryService().createAdapterFactory();
            }
            adapterFactory.setOutputType(adapterOutputType);
            adapterFactory.setMetaType(metaType);
            adapterFactory.configure(appCtx.getServiceContext(), configuration, warningCollector);
            if (metaType == null && configuration.containsKey(ExternalDataConstants.KEY_META_TYPE_NAME)) {
                metaType = getOutputType(feed, configuration.get(ExternalDataConstants.KEY_META_TYPE_NAME));
                if (metaType == null) {
                    throw new AsterixException("Unknown specified feed meta output data type "
                            + configuration.get(ExternalDataConstants.KEY_META_TYPE_NAME));
                }
            }
            if (adapterOutputType == null) {
                if (!configuration.containsKey(ExternalDataConstants.KEY_TYPE_NAME)) {
                    throw new AsterixException("Unspecified feed output data type");
                }
                adapterOutputType = getOutputType(feed, configuration.get(ExternalDataConstants.KEY_TYPE_NAME));
                if (adapterOutputType == null) {
                    throw new AsterixException("Unknown specified feed output data type "
                            + configuration.get(ExternalDataConstants.KEY_TYPE_NAME));
                }
            }
        } catch (Exception e) {
            throw new AsterixException("Invalid feed parameters. Exception Message:" + e.getMessage(), e);
        }
    }

    private static ITypedAdapterFactory createExternalAdapterFactory(MetadataTransactionContext mdTxnCtx,
            DatasourceAdapter adapterEntity, String adapterFactoryClassname)
            throws AlgebricksException, RemoteException, HyracksDataException {
        DataverseName libraryDataverse = adapterEntity.getLibraryDataverseName();
        String libraryName = adapterEntity.getLibraryName();
        Library library = MetadataManager.INSTANCE.getLibrary(mdTxnCtx, libraryDataverse, libraryName);
        if (library == null) {
            throw new CompilationException(ErrorCode.UNKNOWN_LIBRARY, libraryName);
        }
        if (!ExternalFunctionLanguage.JAVA.name().equals(library.getLanguage())) {
            throw new HyracksDataException("Unexpected library language: " + library.getLanguage());
        }
        return new ExternalAdapterFactory(libraryDataverse, libraryName, adapterFactoryClassname);
    }

    @SuppressWarnings("rawtypes")
    public static Triple<ITypedAdapterFactory, RecordDescriptor, AdapterType> getFeedFactoryAndOutput(Feed feed,
            FeedPolicyAccessor policyAccessor, MetadataTransactionContext mdTxnCtx, ICcApplicationContext appCtx)
            throws AlgebricksException {
        // This method needs to be re-visited
        String adapterName = null;
        DatasourceAdapter adapterEntity = null;
        String adapterFactoryClassname = null;
        ITypedAdapterFactory adapterFactory = null;
        ARecordType adapterOutputType = null;
        ARecordType metaType = null;
        Triple<ITypedAdapterFactory, RecordDescriptor, IDataSourceAdapter.AdapterType> feedProps = null;
        IDataSourceAdapter.AdapterType adapterType = null;
        try {
            Map<String, String> configuration = feed.getConfiguration();
            adapterName = configuration.get(ExternalDataConstants.KEY_ADAPTER_NAME);
            configuration.putAll(policyAccessor.getFeedPolicy());
            adapterOutputType = getOutputType(feed, configuration.get(ExternalDataConstants.KEY_TYPE_NAME));
            metaType = getOutputType(feed, configuration.get(ExternalDataConstants.KEY_META_TYPE_NAME));
            ExternalDataUtils.prepareFeed(configuration, feed.getDataverseName(), feed.getFeedName());
            // Get adapter from metadata dataset <Metadata dataverse>
            adapterEntity = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, MetadataConstants.METADATA_DATAVERSE_NAME,
                    adapterName);
            // Get adapter from metadata dataset <The feed dataverse>
            if (adapterEntity == null) {
                adapterEntity = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, feed.getDataverseName(), adapterName);
            }
            if (adapterEntity != null) {
                adapterType = adapterEntity.getType();
                adapterFactoryClassname = adapterEntity.getClassname();
                switch (adapterType) {
                    case INTERNAL:
                        adapterFactory = (ITypedAdapterFactory) Class.forName(adapterFactoryClassname).newInstance();
                        break;
                    case EXTERNAL:
                        adapterFactory = createExternalAdapterFactory(mdTxnCtx, adapterEntity, adapterFactoryClassname);
                        break;
                    default:
                        throw new AsterixException("Unknown Adapter type " + adapterType);
                }
                adapterFactory.setOutputType(adapterOutputType);
                adapterFactory.setMetaType(metaType);
                adapterFactory.configure(appCtx.getServiceContext(), configuration, NoOpWarningCollector.INSTANCE);
            } else {
                adapterFactory = AdapterFactoryProvider.getAdapterFactory(appCtx.getServiceContext(), adapterName,
                        configuration, adapterOutputType, metaType, NoOpWarningCollector.INSTANCE);
                adapterType = IDataSourceAdapter.AdapterType.INTERNAL;
            }
            if (metaType == null) {
                metaType = getOutputType(feed, configuration.get(ExternalDataConstants.KEY_META_TYPE_NAME));
            }
            if (adapterOutputType == null) {
                if (!configuration.containsKey(ExternalDataConstants.KEY_TYPE_NAME)) {
                    throw new AsterixException("Unspecified feed output data type");
                }
                adapterOutputType = getOutputType(feed, configuration.get(ExternalDataConstants.KEY_TYPE_NAME));
            }
            int numOfOutputs = 1;
            if (metaType != null) {
                numOfOutputs++;
            }
            if (ExternalDataUtils.isChangeFeed(configuration)) {
                // get number of PKs
                numOfOutputs += ExternalDataUtils.getNumberOfKeys(configuration);
            }
            ISerializerDeserializer[] serdes = new ISerializerDeserializer[numOfOutputs];
            int i = 0;
            serdes[i++] = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(adapterOutputType);
            if (metaType != null) {
                serdes[i++] = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(metaType);
            }
            if (ExternalDataUtils.isChangeFeed(configuration)) {
                getSerdesForPKs(serdes, configuration, metaType, adapterOutputType, i);
            }
            feedProps = new Triple<>(adapterFactory, new RecordDescriptor(serdes), adapterType);
        } catch (Exception e) {
            throw new AlgebricksException("unable to create adapter", e);
        }
        return feedProps;
    }

    @SuppressWarnings("rawtypes")
    private static void getSerdesForPKs(ISerializerDeserializer[] serdes, Map<String, String> configuration,
            ARecordType metaType, ARecordType adapterOutputType, int index) throws AlgebricksException {
        int[] pkIndexes = ExternalDataUtils.getPKIndexes(configuration);
        if (metaType != null) {
            int[] pkIndicators = ExternalDataUtils.getPKSourceIndicators(configuration);
            for (int j = 0; j < pkIndexes.length; j++) {
                int aInt = pkIndexes[j];
                if (pkIndicators[j] == 0) {
                    serdes[index++] = SerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(adapterOutputType.getFieldTypes()[aInt]);
                } else if (pkIndicators[j] == 1) {
                    serdes[index++] = SerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(metaType.getFieldTypes()[aInt]);
                } else {
                    throw new AlgebricksException("a key source indicator can only be 0 or 1");
                }
            }
        } else {
            for (int aInt : pkIndexes) {
                serdes[index++] = SerializerDeserializerProvider.INSTANCE
                        .getSerializerDeserializer(adapterOutputType.getFieldTypes()[aInt]);
            }
        }
    }

    public static ARecordType getOutputType(IFeed feed, String fqOutputType) throws AlgebricksException {
        if (fqOutputType == null) {
            return null;
        }

        ARecordType outputType = null;
        MetadataTransactionContext ctx = null;
        try {
            ctx = MetadataManager.INSTANCE.beginTransaction();
            Datatype t = MetadataManager.INSTANCE.getDatatype(ctx, feed.getDataverseName(), fqOutputType);
            if (t == null || t.getDatatype().getTypeTag() != ATypeTag.OBJECT) {
                throw new MetadataException(ErrorCode.FEED_METADATA_UTIL_UNEXPECTED_FEED_DATATYPE, fqOutputType);
            }
            outputType = (ARecordType) t.getDatatype();
            MetadataManager.INSTANCE.commitTransaction(ctx);
        } catch (ACIDException | RemoteException e) {
            if (ctx != null) {
                try {
                    MetadataManager.INSTANCE.abortTransaction(ctx);
                } catch (ACIDException | RemoteException e2) {
                    e.addSuppressed(e2);
                }
                throw new MetadataException(ErrorCode.FEED_CREATE_FEED_DATATYPE_ERROR, e, fqOutputType);
            }
        }
        return outputType;
    }
}
