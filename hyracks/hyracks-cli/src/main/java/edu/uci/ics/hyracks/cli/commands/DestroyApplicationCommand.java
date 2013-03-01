package edu.uci.ics.hyracks.cli.commands;

import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.cli.Session;

public class DestroyApplicationCommand extends Command {
    private String appName;

    public DestroyApplicationCommand(String appName) {
        this.appName = appName;
    }

    @Override
    public void run(Session session) throws Exception {
        IHyracksClientConnection hcc = session.getConnection();
        if (hcc == null) {
            throw new RuntimeException("Not connected to Hyracks Cluster Controller");
        }
        System.err.println("Destroying application: " + appName);
        hcc.destroyApplication(appName);
    }
}