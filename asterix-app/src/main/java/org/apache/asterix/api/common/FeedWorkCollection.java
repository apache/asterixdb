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
package org.apache.asterix.api.common;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.api.common.SessionConfig.OutputFormat;
import org.apache.asterix.aql.base.Statement;
import org.apache.asterix.aql.expression.DataverseDecl;
import org.apache.asterix.aql.expression.Identifier;
import org.apache.asterix.aql.expression.SubscribeFeedStatement;
import org.apache.asterix.aql.translator.AqlTranslator;
import org.apache.asterix.common.feeds.FeedConnectionRequest;
import org.apache.asterix.common.feeds.FeedConnectionRequest.ConnectionStatus;
import org.apache.asterix.common.feeds.api.IFeedWork;
import org.apache.asterix.common.feeds.api.IFeedWorkEventListener;
import org.apache.asterix.feeds.FeedCollectInfo;
import org.apache.asterix.om.util.AsterixAppContextInfo;
import org.apache.hyracks.api.job.JobId;

/**
 * A collection of feed management related task, each represented as an implementation of {@code IFeedWork}.
 */
public class FeedWorkCollection {

    private static Logger LOGGER = Logger.getLogger(FeedWorkCollection.class.getName());

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

            private final FeedConnectionRequest request;
            private final String[] locations;

            public SubscribeFeedWorkRunnable(String[] locations, FeedConnectionRequest request) {
                this.request = request;
                this.locations = locations;
            }

            @Override
            public void run() {
                try {
                    PrintWriter writer = new PrintWriter(System.out, true);
                    SessionConfig pc = new SessionConfig(writer, OutputFormat.ADM);
                    DataverseDecl dataverseDecl = new DataverseDecl(new Identifier(request.getReceivingFeedId()
                            .getDataverse()));
                    SubscribeFeedStatement subscribeStmt = new SubscribeFeedStatement(locations, request);
                    List<Statement> statements = new ArrayList<Statement>();
                    statements.add(dataverseDecl);
                    statements.add(subscribeStmt);
                    AqlTranslator translator = new AqlTranslator(statements, pc);
                    translator.compileAndExecute(AsterixAppContextInfo.getInstance().getHcc(), null, AqlTranslator.ResultDelivery.SYNC);
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Submitted connection requests for execution: " + request);
                    }
                } catch (Exception e) {
                    if (LOGGER.isLoggable(Level.SEVERE)) {
                        LOGGER.severe("Exception in executing " + request);
                    }
                    throw new RuntimeException(e);
                }
            }
        }

        public static class FeedSubscribeWorkEventListener implements IFeedWorkEventListener {

            @Override
            public void workFailed(IFeedWork work, Exception e) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning(" Feed subscription request " + ((SubscribeFeedWork) work).request
                            + " failed with exception " + e);
                }
            }

            @Override
            public void workCompleted(IFeedWork work) {
                ((SubscribeFeedWork) work).request.setSubscriptionStatus(ConnectionStatus.ACTIVE);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.warning(" Feed subscription request " + ((SubscribeFeedWork) work).request + " completed ");
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

    /**
     * The task of activating a set of feeds.
     */
    public static class ActivateFeedWork implements IFeedWork {

        private final Runnable runnable;

        @Override
        public Runnable getRunnable() {
            return runnable;
        }

        public ActivateFeedWork(List<FeedCollectInfo> feedsToRevive) {
            this.runnable = new FeedsActivateRunnable(feedsToRevive);
        }

        public ActivateFeedWork() {
            this.runnable = new FeedsActivateRunnable();
        }

        private static class FeedsActivateRunnable implements Runnable {

            private List<FeedCollectInfo> feedsToRevive;
            private Mode mode;

            public enum Mode {
                REVIVAL_POST_NODE_REJOIN
            }

            public FeedsActivateRunnable(List<FeedCollectInfo> feedsToRevive) {
                this.feedsToRevive = feedsToRevive;
            }

            public FeedsActivateRunnable() {
            }

            @Override
            public void run() {
                switch (mode) {
                    case REVIVAL_POST_NODE_REJOIN:
                        try {
                            Thread.sleep(10000);
                        } catch (InterruptedException e1) {
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.info("Attempt to resume feed interrupted");
                            }
                            throw new IllegalStateException(e1.getMessage());
                        }
                        for (FeedCollectInfo finfo : feedsToRevive) {
                            try {
                                JobId jobId = AsterixAppContextInfo.getInstance().getHcc().startJob(finfo.jobSpec);
                                if (LOGGER.isLoggable(Level.INFO)) {
                                    LOGGER.info("Resumed feed :" + finfo.feedConnectionId + " job id " + jobId);
                                    LOGGER.info("Job:" + finfo.jobSpec);
                                }
                            } catch (Exception e) {
                                if (LOGGER.isLoggable(Level.WARNING)) {
                                    LOGGER.warning("Unable to resume feed " + finfo.feedConnectionId + " "
                                            + e.getMessage());
                                }
                            }
                        }
                }
            }

        }

    }
}
