package org.apache.asterix.active;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.active.ActiveJobInfo;
import org.apache.asterix.om.util.AsterixAppContextInfo;
import org.apache.hyracks.api.job.JobId;

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
public class ActiveJobsActivator implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(ActiveJobsActivator.class.getName());

    private List<ActiveJobInfo> objectsToRevive;
    private Mode mode;

    public enum Mode {
        REVIVAL_POST_CLUSTER_REBOOT,
        REVIVAL_POST_NODE_REJOIN
    }

    public ActiveJobsActivator() {
        this.mode = Mode.REVIVAL_POST_CLUSTER_REBOOT;
    }

    public ActiveJobsActivator(List<ActiveJobInfo> objectsToRevive) {
        this.objectsToRevive = objectsToRevive;
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
                for (ActiveJobInfo finfo : objectsToRevive) {
                    try {
                        JobId jobId = AsterixAppContextInfo.getInstance().getHcc().startJob(finfo.getSpec());
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("Resumed :" + finfo.getActiveJobId() + " job id " + jobId);
                            LOGGER.info("Job:" + finfo.getSpec());
                        }
                    } catch (Exception e) {
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("Unable to resume " + finfo.getActiveJobId() + " " + e.getMessage());
                        }
                    }
                }
        }
    }
    //not currently in use
    /*
        public void reviveFeed(String dataverse, String feedName, String dataset, String feedPolicy) {
            PrintWriter writer = new PrintWriter(System.out, true);
            SessionConfig pc = new SessionConfig(writer, SessionConfig.OutputFormat.ADM);
            try {
                DataverseDecl dataverseDecl = new DataverseDecl(new Identifier(dataverse));
                ConnectFeedStatement stmt = new ConnectFeedStatement(new Identifier(dataverse), new Identifier(feedName),
                        new Identifier(dataset), feedPolicy, 0);
                stmt.setForceConnect(true);
                List<Statement> statements = new ArrayList<Statement>();
                statements.add(dataverseDecl);
                statements.add(stmt);
                AqlTranslator translator = new AqlTranslator(statements, pc);
                translator.compileAndExecute(AsterixAppContextInfo.getInstance().getHcc(), null, ResultDelivery.SYNC);
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
        */
}