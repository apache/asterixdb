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
package edu.uci.ics.asterix.aql.expression;

import java.io.StringReader;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.aql.parser.AQLParser;
import edu.uci.ics.asterix.aql.parser.ParseException;
import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedActivity;
import edu.uci.ics.asterix.common.feeds.FeedConnectionRequest;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.FeedPolicyAccessor;
import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.entities.DatasourceAdapter.AdapterType;
import edu.uci.ics.asterix.metadata.entities.Feed;
import edu.uci.ics.asterix.metadata.entities.Function;
import edu.uci.ics.asterix.metadata.entities.PrimaryFeed;
import edu.uci.ics.asterix.metadata.entities.SecondaryFeed;
import edu.uci.ics.asterix.metadata.feeds.FeedUtil;
import edu.uci.ics.asterix.metadata.feeds.IFeedAdapterFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;

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
        Feed subscriberFeed = MetadataManager.INSTANCE.getFeed(mdTxnCtx, connectionRequest.getReceivingFeedId()
                .getDataverse(), connectionRequest.getReceivingFeedId().getFeedName());
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
                throw new MetadataException(" Incompatible function: " + appliedFunction
                        + " Number if arguments must be 1");
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
                        builder.append(" let " + "$" + lValueName + variableIndex + ":=(" + function.getFunctionBody()
                                + ")");
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
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return null;
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
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
