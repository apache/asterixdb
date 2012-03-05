/*
 * Copyright 2009-2011 by The Regents of the University of California
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

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.feed.comm.AlterFeedMessage;
import edu.uci.ics.asterix.feed.comm.FeedMessage;
import edu.uci.ics.asterix.feed.comm.IFeedMessage;
import edu.uci.ics.asterix.feed.comm.IFeedMessage.MessageType;
import edu.uci.ics.asterix.formats.base.IDataFormat;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledFeedDatasetDetails;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.translator.DmlTranslator.CompiledControlFeedStatement;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.operators.std.AssignRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.core.api.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.core.api.constraints.AlgebricksPartitionConstraintHelper;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.utils.Triple;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.misc.NullSinkOperatorDescriptor;

public class FeedOperations {

    private static final Logger LOGGER = Logger.getLogger(IndexOperations.class.getName());

    public static JobSpecification buildControlFeedJobSpec(CompiledControlFeedStatement controlFeedStatement,
            AqlCompiledMetadataDeclarations datasetDecls) throws AsterixException, AlgebricksException {
        switch (controlFeedStatement.getOperationType()) {
            case ALTER:
            case SUSPEND:
            case RESUME:
            case END: {
                return createSendMessageToFeedJobSpec(controlFeedStatement, datasetDecls);
            }
            default: {
                throw new AsterixException("Unknown Operation Type: " + controlFeedStatement.getOperationType());
            }

        }
    }

    private static JobSpecification createSendMessageToFeedJobSpec(CompiledControlFeedStatement controlFeedStatement,
            AqlCompiledMetadataDeclarations metadata) throws AsterixException {
        String datasetName = controlFeedStatement.getDatasetName().getValue();
        String datasetPath = metadata.getRelativePath(datasetName);

        LOGGER.info(" DATASETPATH: " + datasetPath);

        AqlCompiledDatasetDecl adecl = metadata.findDataset(datasetName);
        if (adecl == null) {
            throw new AsterixException("FEED DATASET: No metadata for dataset " + datasetName);
        }
        if (adecl.getDatasetType() != DatasetType.FEED) {
            throw new AsterixException("Operation not support for dataset type  " + adecl.getDatasetType());
        }

        JobSpecification spec = new JobSpecification();
        IOperatorDescriptor feedMessenger;
        AlgebricksPartitionConstraint messengerPc;

        List<IFeedMessage> feedMessages = new ArrayList<IFeedMessage>();
        switch (controlFeedStatement.getOperationType()) {
            case SUSPEND:
                feedMessages.add(new FeedMessage(MessageType.SUSPEND));
                break;
            case END:
                feedMessages.add(new FeedMessage(MessageType.STOP));
                break;
            case RESUME:
                feedMessages.add(new FeedMessage(MessageType.RESUME));
                break;
            case ALTER:
                feedMessages.add(new AlterFeedMessage(controlFeedStatement.getProperties()));
                break;
        }

        try {
            Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> p = AqlMetadataProvider.buildFeedMessengerRuntime(
                    spec, metadata, (AqlCompiledFeedDatasetDetails) adecl.getAqlCompiledDatasetDetails(),
                    metadata.getDataverseName(), datasetName, feedMessages);
            feedMessenger = p.first;
            messengerPc = p.second;
        } catch (AlgebricksException e) {
            throw new AsterixException(e);
        }

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, feedMessenger, messengerPc);

        NullSinkOperatorDescriptor nullSink = new NullSinkOperatorDescriptor(spec);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, nullSink, messengerPc);

        spec.connect(new OneToOneConnectorDescriptor(spec), feedMessenger, 0, nullSink, 0);

        spec.addRoot(nullSink);
        return spec;

    }

    private static AssignRuntimeFactory makeAssignRuntimeFactory(AqlCompiledDatasetDecl compiledDatasetDecl) {
        List<Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType>> partitioningFunctions = DatasetUtils
                .getPartitioningFunctions(compiledDatasetDecl);
        int numKeys = partitioningFunctions.size();
        IEvaluatorFactory[] evalFactories = new IEvaluatorFactory[numKeys];

        int index = 0;
        for (Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> evalFactoryAndType : partitioningFunctions) {
            evalFactories[index++] = evalFactoryAndType.first;
        }

        int[] outColumns = new int[numKeys];
        int[] projectionList = new int[numKeys + 1];
        projectionList[0] = 0;

        for (int i = 0; i < numKeys; i++) {
            outColumns[i] = i + 1;
            projectionList[i + 1] = i + 1;
        }
        return new AssignRuntimeFactory(outColumns, evalFactories, projectionList);
    }

    @SuppressWarnings("unchecked")
    private static RecordDescriptor computePayloadKeyRecordDescriptor(AqlCompiledDatasetDecl compiledDatasetDecl,
            ISerializerDeserializer payloadSerde, IDataFormat dataFormat) throws AlgebricksException {

        List<Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType>> partitioningFunctions = DatasetUtils
                .getPartitioningFunctions(compiledDatasetDecl);
        int numKeys = partitioningFunctions.size();
        ISerializerDeserializer[] recordFields = new ISerializerDeserializer[1 + numKeys];
        recordFields[0] = payloadSerde;
        int index = 0;
        for (Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> evalFactoryAndType : partitioningFunctions) {
            IAType keyType = evalFactoryAndType.third;
            ISerializerDeserializer keySerde = dataFormat.getSerdeProvider().getSerializerDeserializer(keyType);
            recordFields[index + 1] = keySerde;
            index++;
        }
        return new RecordDescriptor(recordFields);
    }

    private static String stringOf(FileSplit fs) {
        return fs.getNodeName() + ":" + fs.getLocalFile().toString();
    }
}
