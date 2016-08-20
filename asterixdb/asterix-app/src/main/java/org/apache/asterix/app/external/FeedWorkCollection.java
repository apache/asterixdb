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
package org.apache.asterix.app.external;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.app.translator.DefaultStatementExecutorFactory;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.common.app.SessionConfig;
import org.apache.asterix.common.app.SessionConfig.OutputFormat;
import org.apache.asterix.compiler.provider.AqlCompilationProvider;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.external.feed.api.IFeedWork;
import org.apache.asterix.external.feed.api.IFeedWorkEventListener;
import org.apache.asterix.external.feed.management.FeedConnectionRequest;
import org.apache.asterix.external.feed.management.FeedConnectionRequest.ConnectionStatus;
import org.apache.asterix.lang.aql.statement.SubscribeFeedStatement;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.DataverseDecl;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.om.util.AsterixAppContextInfo;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * A collection of feed management related task, each represented as an implementation of {@code IFeedWork}.
 */
public class FeedWorkCollection {

    private static Logger LOGGER = Logger.getLogger(FeedWorkCollection.class.getName());
    private static final ILangCompilationProvider compilationProvider = new AqlCompilationProvider();

    /**
     * The task of subscribing to a feed to obtain data.
     */
    public static class SubscribeFeedWork implements IFeedWork {

        private final Runnable runnable;

        private final FeedConnectionRequest request;

        @Override
        public Runnable getRunnable() {
            return runnable;
        }

        public SubscribeFeedWork(String[] locations, FeedConnectionRequest request) {
            this.runnable = new SubscribeFeedWorkRunnable(locations, request);
            this.request = request;
        }

        private static class SubscribeFeedWorkRunnable implements Runnable {

            private static final DefaultStatementExecutorFactory qtFactory = new DefaultStatementExecutorFactory(null);
            private final FeedConnectionRequest request;
            private final String[] locations;

            public SubscribeFeedWorkRunnable(String[] locations, FeedConnectionRequest request) {
                this.request = request;
                this.locations = locations;
            }

            @Override
            public void run() {
                try {
                    //TODO(amoudi): route PrintWriter to log file
                    PrintWriter writer = new PrintWriter(System.err, true);
                    SessionConfig pc = new SessionConfig(writer, OutputFormat.ADM);
                    DataverseDecl dataverseDecl = new DataverseDecl(
                            new Identifier(request.getReceivingFeedId().getDataverse()));
                    SubscribeFeedStatement subscribeStmt = new SubscribeFeedStatement(locations, request);
                    List<Statement> statements = new ArrayList<Statement>();
                    statements.add(dataverseDecl);
                    statements.add(subscribeStmt);
                    IStatementExecutor translator = qtFactory.create(statements, pc, compilationProvider);
                    translator.compileAndExecute(AsterixAppContextInfo.getInstance().getHcc(), null,
                            QueryTranslator.ResultDelivery.SYNC);
                    if (LOGGER.isEnabledFor(Level.INFO)) {
                        LOGGER.info("Submitted connection requests for execution: " + request);
                    }
                } catch (Exception e) {
                    if (LOGGER.isEnabledFor(Level.FATAL)) {
                        LOGGER.fatal("Exception in executing " + request, e);
                    }
                }
            }
        }

        public static class FeedSubscribeWorkEventListener implements IFeedWorkEventListener {

            @Override
            public void workFailed(IFeedWork work, Exception e) {
                if (LOGGER.isEnabledFor(Level.WARN)) {
                    LOGGER.warn(" Feed subscription request " + ((SubscribeFeedWork) work).request
                            + " failed with exception " + e);
                }
            }

            @Override
            public void workCompleted(IFeedWork work) {
                ((SubscribeFeedWork) work).request.setSubscriptionStatus(ConnectionStatus.ACTIVE);
                if (LOGGER.isEnabledFor(Level.INFO)) {
                    LOGGER.info(" Feed subscription request " + ((SubscribeFeedWork) work).request + " completed ");
                }
            }
        }

        public FeedConnectionRequest getRequest() {
            return request;
        }

        @Override
        public String toString() {
            return "SubscribeFeedWork for [" + request + "]";
        }
    }
}
