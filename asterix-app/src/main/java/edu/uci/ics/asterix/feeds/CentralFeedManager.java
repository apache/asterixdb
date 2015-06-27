/*
 * Copyright 2009-2014 by The Regents of the University of California
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
package edu.uci.ics.asterix.feeds;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.List;

import edu.uci.ics.asterix.api.common.SessionConfig;
import edu.uci.ics.asterix.api.common.SessionConfig.OutputFormat;
import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.parser.AQLParser;
import edu.uci.ics.asterix.aql.translator.AqlTranslator;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.api.ICentralFeedManager;
import edu.uci.ics.asterix.common.feeds.api.IFeedLoadManager;
import edu.uci.ics.asterix.common.feeds.api.IFeedTrackingManager;
import edu.uci.ics.asterix.metadata.feeds.SocketMessageListener;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class CentralFeedManager implements ICentralFeedManager {

    private static final ICentralFeedManager centralFeedManager = new CentralFeedManager();

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

        public static void executeAQL(String aql) throws Exception {
            AQLParser parser = new AQLParser(new StringReader(aql));
            List<Statement> statements;
            statements = parser.Statement();
            SessionConfig pc = new SessionConfig(out, OutputFormat.ADM);
            AqlTranslator translator = new AqlTranslator(statements, pc);
            translator.compileAndExecute(AsterixAppContextInfo.getInstance().getHcc(), null, AqlTranslator.ResultDelivery.SYNC);
        }
    }

}
