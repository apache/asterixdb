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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.List;

import org.apache.asterix.api.common.SessionConfig;
import org.apache.asterix.api.common.SessionConfig.OutputFormat;
import org.apache.asterix.aql.base.Statement;
import org.apache.asterix.aql.parser.AQLParser;
import org.apache.asterix.aql.translator.AqlTranslator;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.feeds.api.ICentralActiveManager;
import org.apache.asterix.common.feeds.api.IActiveLoadManager;
import org.apache.asterix.common.feeds.api.IFeedTrackingManager;
import org.apache.asterix.metadata.feeds.SocketMessageListener;
import org.apache.asterix.om.util.AsterixAppContextInfo;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

public class CentralActiveManager implements ICentralActiveManager {

    private static final ICentralActiveManager centralFeedManager = new CentralActiveManager();

    public static ICentralActiveManager getInstance() {
        return centralFeedManager;
    }

    private final int port;
    private final IActiveLoadManager feedLoadManager;
    private final IFeedTrackingManager feedTrackingManager;
    private final SocketMessageListener messageListener;

    private CentralActiveManager() {
        this.port = AsterixAppContextInfo.getInstance().getFeedProperties().getFeedCentralManagerPort();
        this.feedLoadManager = new ActiveLoadManager();
        this.feedTrackingManager = new FeedTrackingManager();
        this.messageListener = new SocketMessageListener(port, new FeedMessageReceiver(this));
    }

    @Override
    public void start() throws AsterixException {
        messageListener.start();
    }

    @Override
    public void stop() throws AsterixException, IOException {
        messageListener.stop();
    }

    public static JobId runJob(JobSpecification spec, boolean waitForCompletion) throws Exception {
        IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
        JobId jobId = hcc.startJob(spec);
        if (waitForCompletion) {
            hcc.waitForCompletion(jobId);
        }
        return jobId;
    }

    @Override
    public IActiveLoadManager getFeedLoadManager() {
        return feedLoadManager;
    }

    @Override
    public IFeedTrackingManager getFeedTrackingManager() {
        return feedTrackingManager;
    }

    public static class AQLExecutor {

        private static final PrintWriter out = new PrintWriter(System.out, true);

        public static void executeAQL(String aql) throws Exception {
            AQLParser parser = new AQLParser(new StringReader(aql));
            List<Statement> statements;
            statements = parser.Statement();
            SessionConfig pc = new SessionConfig(out, OutputFormat.ADM);
            AqlTranslator translator = new AqlTranslator(statements, pc);
            translator.compileAndExecute(AsterixAppContextInfo.getInstance().getHcc(), null,
                    AqlTranslator.ResultDelivery.SYNC);
        }
    }

}
