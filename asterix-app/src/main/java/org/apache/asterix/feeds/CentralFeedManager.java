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
import org.apache.asterix.aql.translator.QueryTranslator;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.feeds.api.ICentralFeedManager;
import org.apache.asterix.common.feeds.api.IFeedLoadManager;
import org.apache.asterix.common.feeds.api.IFeedTrackingManager;
import org.apache.asterix.compiler.provider.AqlCompilationProvider;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.lang.aql.parser.AQLParserFactory;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.metadata.feeds.SocketMessageListener;
import org.apache.asterix.om.util.AsterixAppContextInfo;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

public class CentralFeedManager implements ICentralFeedManager {

    private static final ICentralFeedManager centralFeedManager = new CentralFeedManager();
    private static final ILangCompilationProvider compilationProvider = new AqlCompilationProvider();

    public static ICentralFeedManager getInstance() {
        return centralFeedManager;
    }

    private final int port;
    private final IFeedLoadManager feedLoadManager;
    private final IFeedTrackingManager feedTrackingManager;
    private final SocketMessageListener messageListener;

    private CentralFeedManager() {
        this.port = AsterixAppContextInfo.getInstance().getFeedProperties().getFeedCentralManagerPort();
        this.feedLoadManager = new FeedLoadManager();
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
    public IFeedLoadManager getFeedLoadManager() {
        return feedLoadManager;
    }

    @Override
    public IFeedTrackingManager getFeedTrackingManager() {
        return feedTrackingManager;
    }

    public static class AQLExecutor {

        private static final PrintWriter out = new PrintWriter(System.out, true);
        private static final IParserFactory parserFactory = new AQLParserFactory();

        public static void executeAQL(String aql) throws Exception {
            IParser parser = parserFactory.createParser(new StringReader(aql));
            List<Statement> statements = parser.parse();
            SessionConfig pc = new SessionConfig(out, OutputFormat.ADM);
            QueryTranslator translator = new QueryTranslator(statements, pc, compilationProvider);
            translator.compileAndExecute(AsterixAppContextInfo.getInstance().getHcc(), null,
                    QueryTranslator.ResultDelivery.SYNC);
        }
    }

}
