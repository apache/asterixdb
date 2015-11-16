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
package org.apache.asterix.feeds;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.api.common.SessionConfig;
import org.apache.asterix.api.common.SessionConfig.OutputFormat;
import org.apache.asterix.aql.translator.QueryTranslator;
import org.apache.asterix.compiler.provider.AqlCompilationProvider;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.ConnectFeedStatement;
import org.apache.asterix.lang.common.statement.DataverseDecl;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.om.util.AsterixAppContextInfo;
import org.apache.hyracks.api.job.JobId;

public class FeedsActivator implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(FeedJobNotificationHandler.class.getName());
    private static final ILangCompilationProvider compilationProvider = new AqlCompilationProvider();

    private List<FeedCollectInfo> feedsToRevive;
    private Mode mode;

    public enum Mode {
        REVIVAL_POST_CLUSTER_REBOOT,
        REVIVAL_POST_NODE_REJOIN
    }

    public FeedsActivator() {
        this.mode = Mode.REVIVAL_POST_CLUSTER_REBOOT;
    }

    public FeedsActivator(List<FeedCollectInfo> feedsToRevive) {
        this.feedsToRevive = feedsToRevive;
        this.mode = Mode.REVIVAL_POST_NODE_REJOIN;
    }

    @Override
    public void run() {
        switch (mode) {
            case REVIVAL_POST_CLUSTER_REBOOT:
                //revivePostClusterReboot();
                break;
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
                            LOGGER.warning("Unable to resume feed " + finfo.feedConnectionId + " " + e.getMessage());
                        }
                    }
                }
        }
    }

    public void reviveFeed(String dataverse, String feedName, String dataset, String feedPolicy) {
        PrintWriter writer = new PrintWriter(System.out, true);
        SessionConfig pc = new SessionConfig(writer, OutputFormat.ADM);
        try {
            DataverseDecl dataverseDecl = new DataverseDecl(new Identifier(dataverse));
            ConnectFeedStatement stmt = new ConnectFeedStatement(new Identifier(dataverse), new Identifier(feedName),
                    new Identifier(dataset), feedPolicy, 0);
            stmt.setForceConnect(true);
            List<Statement> statements = new ArrayList<Statement>();
            statements.add(dataverseDecl);
            statements.add(stmt);
            QueryTranslator translator = new QueryTranslator(statements, pc, compilationProvider);
            translator.compileAndExecute(AsterixAppContextInfo.getInstance().getHcc(), null,
                    QueryTranslator.ResultDelivery.SYNC);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Resumed feed: " + dataverse + ":" + dataset + " using policy " + feedPolicy);
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Exception in resuming loser feed: " + dataverse + ":" + dataset + " using policy "
                        + feedPolicy + " Exception " + e.getMessage());
            }
        }
    }
}