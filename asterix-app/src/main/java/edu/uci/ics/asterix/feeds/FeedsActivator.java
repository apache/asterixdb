package edu.uci.ics.asterix.feeds;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.api.common.SessionConfig;
import edu.uci.ics.asterix.api.common.SessionConfig.OutputFormat;
import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.ConnectFeedStatement;
import edu.uci.ics.asterix.aql.expression.DataverseDecl;
import edu.uci.ics.asterix.aql.expression.Identifier;
import edu.uci.ics.asterix.aql.translator.AqlTranslator;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.hyracks.api.job.JobId;

public class FeedsActivator implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(FeedJobNotificationHandler.class.getName());

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
            AqlTranslator translator = new AqlTranslator(statements, pc);
            translator.compileAndExecute(AsterixAppContextInfo.getInstance().getHcc(), null, AqlTranslator.ResultDelivery.SYNC);
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