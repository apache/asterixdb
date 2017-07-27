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

import org.apache.asterix.active.EntityId;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.external.feed.management.FeedConnectionRequest;
import org.apache.asterix.external.feed.watch.FeedActivityDetails;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.lang.aql.parser.AQLParserFactory;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.InsertStatement;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.feeds.FeedMetadataUtil;

/**
 * Represents the AQL statement for subscribing to a feed.
 * This AQL statement is private and may not be used by the end-user.
 */
public class SubscribeFeedStatement implements Statement {

    public static final String WAIT_FOR_COMPLETION = "wait-for-completion-feed";
    private static final Integer INSERT_STATEMENT_POS = 3;
    private static final Logger LOGGER = Logger.getLogger(SubscribeFeedStatement.class.getName());
    private final int varCounter;
    private final String[] locations;
    private final FeedConnectionRequest connectionRequest;
    private final IParserFactory parserFactory = new AQLParserFactory();
    private Query query;

    public SubscribeFeedStatement(String[] locations, FeedConnectionRequest subscriptionRequest) {
        this.connectionRequest = subscriptionRequest;
        this.varCounter = 0;
        this.locations = locations;
    }

    public void initialize(MetadataTransactionContext mdTxnCtx) throws MetadataException {
        this.query = new Query(false);
        EntityId sourceFeedId = connectionRequest.getReceivingFeedId();
        Feed subscriberFeed =
                MetadataManager.INSTANCE.getFeed(mdTxnCtx, connectionRequest.getReceivingFeedId().getDataverse(),
                        connectionRequest.getReceivingFeedId().getEntityName());
        if (subscriberFeed == null) {
            throw new IllegalStateException(" Subscriber feed " + subscriberFeed + " not found.");
        }

        String feedOutputType = getOutputType(mdTxnCtx);
        StringBuilder builder = new StringBuilder();
        builder.append("use dataverse " + sourceFeedId.getDataverse() + ";\n");
        builder.append("set" + " " + FunctionUtil.IMPORT_PRIVATE_FUNCTIONS + " " + "'" + Boolean.TRUE + "'" + ";\n");
        builder.append("set" + " " + FeedActivityDetails.FEED_POLICY_NAME + " " + "'" + connectionRequest.getPolicy()
                + "'" + ";\n");

        builder.append("insert into dataset " + connectionRequest.getTargetDataset() + " ");
        builder.append(" (" + " for $x in feed-collect ('" + sourceFeedId.getDataverse() + "'" + "," + "'"
                + sourceFeedId.getEntityName() + "'" + "," + "'"
                + connectionRequest.getReceivingFeedId().getEntityName() + "'" + "," + "'"
                + connectionRequest.getSubscriptionLocation().name() + "'" + "," + "'"
                + connectionRequest.getTargetDataset() + "'" + "," + "'" + feedOutputType + "'" + ")");

        List<FunctionSignature> functionsToApply = connectionRequest.getFunctionsToApply();
        if ((functionsToApply != null) && functionsToApply.isEmpty()) {
            builder.append(" return $x");
        } else {
            Function function;
            String rValueName = "x";
            String lValueName = "y";
            int variableIndex = 0;
            for (FunctionSignature appliedFunction : functionsToApply) {
                function = MetadataManager.INSTANCE.getFunction(mdTxnCtx, appliedFunction);
                variableIndex++;
                switch (function.getLanguage().toUpperCase()) {
                    case Function.LANGUAGE_AQL:
                        builder.append(" let " + "$" + lValueName + variableIndex + ":=" + function.getName() + "("
                                + "$" + rValueName + ")");
                        rValueName = lValueName + variableIndex;
                        break;
                    case Function.LANGUAGE_JAVA:
                        builder.append(" let " + "$" + lValueName + variableIndex + ":=" + function.getName() + "("
                                + "$" + rValueName + ")");
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
        IParser parser = parserFactory.createParser(new StringReader(builder.toString()));

        List<Statement> statements;
        try {
            statements = parser.parse();
            query = ((InsertStatement) statements.get(INSERT_STATEMENT_POS)).getQuery();
        } catch (CompilationException pe) {
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
    public byte getKind() {
        return Statement.Kind.SUBSCRIBE_FEED;
    }

    public String getPolicy() {
        return connectionRequest.getPolicy();
    }

    public FeedConnectionRequest getSubscriptionRequest() {
        return connectionRequest;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return null;
    }

    public String getDataverseName() {
        return connectionRequest.getReceivingFeedId().getDataverse();
    }

    private String getOutputType(MetadataTransactionContext mdTxnCtx) throws MetadataException {
        String outputType;
        EntityId feedId = connectionRequest.getReceivingFeedId();
        Feed feed = MetadataManager.INSTANCE.getFeed(mdTxnCtx, feedId.getDataverse(), feedId.getEntityName());
        try {
            outputType = FeedMetadataUtil
                    .getOutputType(feed, feed.getAdapterConfiguration(), ExternalDataConstants.KEY_TYPE_NAME)
                    .getTypeName();
            return outputType;

        } catch (MetadataException ae) {
            throw new MetadataException(ae);
        }
    }

    public String[] getLocations() {
        return locations;
    }

    @Override
    public byte getCategory() {
        return Category.PROCEDURE;
    }
}
