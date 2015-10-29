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
package org.apache.asterix.lang.aql.statement;

import java.io.StringReader;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.feeds.FeedActivity;
import org.apache.asterix.common.feeds.FeedConnectionRequest;
import org.apache.asterix.common.feeds.FeedId;
import org.apache.asterix.common.feeds.FeedPolicyAccessor;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.aql.parser.AQLParser;
import org.apache.asterix.lang.aql.parser.ParseException;
import org.apache.asterix.lang.aql.util.FunctionUtils;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.InsertStatement;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.entities.DatasourceAdapter.AdapterType;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.entities.PrimaryFeed;
import org.apache.asterix.metadata.entities.SecondaryFeed;
import org.apache.asterix.metadata.feeds.FeedUtil;
import org.apache.asterix.metadata.feeds.IFeedAdapterFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;

/**
 * Represents the AQL statement for subscribing to a feed.
 * This AQL statement is private and may not be used by the end-user.
 */
public class SubscribeFeedStatement implements Statement {

    private static final Logger LOGGER = Logger.getLogger(SubscribeFeedStatement.class.getName());
    private final FeedConnectionRequest connectionRequest;
    private Query query;
    private int varCounter;
    private final String[] locations;

    public static final String WAIT_FOR_COMPLETION = "wait-for-completion-feed";

    public SubscribeFeedStatement(String[] locations, FeedConnectionRequest subscriptionRequest) {
        this.connectionRequest = subscriptionRequest;
        this.varCounter = 0;
        this.locations = locations;
    }

    public void initialize(MetadataTransactionContext mdTxnCtx) throws MetadataException {
        this.query = new Query();
        FeedId sourceFeedId = connectionRequest.getFeedJointKey().getFeedId();
        Feed subscriberFeed = MetadataManager.INSTANCE.getFeed(mdTxnCtx,
                connectionRequest.getReceivingFeedId().getDataverse(),
                connectionRequest.getReceivingFeedId().getFeedName());
        if (subscriberFeed == null) {
            throw new IllegalStateException(" Subscriber feed " + subscriberFeed + " not found.");
        }

        String feedOutputType = getOutputType(mdTxnCtx);
        FunctionSignature appliedFunction = subscriberFeed.getAppliedFunction();
        Function function = null;
        if (appliedFunction != null) {
            function = MetadataManager.INSTANCE.getFunction(mdTxnCtx, appliedFunction);
            if (function == null) {
                throw new MetadataException(" Unknown function " + function);
            } else if (function.getParams().size() > 1) {
                throw new MetadataException(
                        " Incompatible function: " + appliedFunction + " Number if arguments must be 1");
            }
        }

        StringBuilder builder = new StringBuilder();
        builder.append("use dataverse " + sourceFeedId.getDataverse() + ";\n");
        builder.append("set" + " " + FunctionUtils.IMPORT_PRIVATE_FUNCTIONS + " " + "'" + Boolean.TRUE + "'" + ";\n");
        builder.append("set" + " " + FeedActivity.FeedActivityDetails.FEED_POLICY_NAME + " " + "'"
                + connectionRequest.getPolicy() + "'" + ";\n");

        builder.append("insert into dataset " + connectionRequest.getTargetDataset() + " ");
        builder.append(" (" + " for $x in feed-collect ('" + sourceFeedId.getDataverse() + "'" + "," + "'"
                + sourceFeedId.getFeedName() + "'" + "," + "'" + connectionRequest.getReceivingFeedId().getFeedName()
                + "'" + "," + "'" + connectionRequest.getSubscriptionLocation().name() + "'" + "," + "'"
                + connectionRequest.getTargetDataset() + "'" + "," + "'" + feedOutputType + "'" + ")");

        List<String> functionsToApply = connectionRequest.getFunctionsToApply();
        if (functionsToApply != null && functionsToApply.isEmpty()) {
            builder.append(" return $x");
        } else {
            String rValueName = "x";
            String lValueName = "y";
            int variableIndex = 0;
            for (String functionName : functionsToApply) {
                function = MetadataManager.INSTANCE.getFunction(mdTxnCtx, appliedFunction);
                variableIndex++;
                switch (function.getLanguage().toUpperCase()) {
                    case Function.LANGUAGE_AQL:
                        builder.append(
                                " let " + "$" + lValueName + variableIndex + ":=(" + function.getFunctionBody() + ")");
                        builder.append("\n");
                        break;
                    case Function.LANGUAGE_JAVA:
                        builder.append(" let " + "$" + lValueName + variableIndex + ":=" + functionName + "(" + "$"
                                + rValueName + ")");
                        rValueName = lValueName + variableIndex;
                        break;
                }
                builder.append("\n");
            }
            builder.append("return $" + lValueName + variableIndex);
        }
        builder.append(")");
        builder.append(";");
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Connect feed statement translated to\n" + builder.toString());
        }
        AQLParser parser = new AQLParser(new StringReader(builder.toString()));

        List<Statement> statements;
        try {
            statements = parser.Statement();
            query = ((InsertStatement) statements.get(3)).getQuery();
        } catch (ParseException pe) {
            throw new MetadataException(pe);
        }

    }

    public Query getQuery() {
        return query;
    }

    public int getVarCounter() {
        return varCounter;
    }

    @Override
    public Kind getKind() {
        return Kind.SUBSCRIBE_FEED;
    }

    public String getPolicy() {
        return connectionRequest.getPolicy();
    }

    public FeedConnectionRequest getSubscriptionRequest() {
        return connectionRequest;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws AsterixException {
        return null;
    }

    public String getDataverseName() {
        return connectionRequest.getReceivingFeedId().getDataverse();
    }

    private String getOutputType(MetadataTransactionContext mdTxnCtx) throws MetadataException {
        String outputType = null;
        FeedId feedId = connectionRequest.getReceivingFeedId();
        Feed feed = MetadataManager.INSTANCE.getFeed(mdTxnCtx, feedId.getDataverse(), feedId.getFeedName());
        FeedPolicyAccessor policyAccessor = new FeedPolicyAccessor(connectionRequest.getPolicyParameters());
        try {
            switch (feed.getFeedType()) {
                case PRIMARY:
                    Triple<IFeedAdapterFactory, ARecordType, AdapterType> factoryOutput = null;

                    factoryOutput = FeedUtil.getPrimaryFeedFactoryAndOutput((PrimaryFeed) feed, policyAccessor,
                            mdTxnCtx);
                    outputType = factoryOutput.second.getTypeName();
                    break;
                case SECONDARY:
                    outputType = FeedUtil.getSecondaryFeedOutput((SecondaryFeed) feed, policyAccessor, mdTxnCtx);
                    break;
            }
            return outputType;

        } catch (AlgebricksException ae) {
            throw new MetadataException(ae);
        }
    }

    public String[] getLocations() {
        return locations;
    }
}
