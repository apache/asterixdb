package edu.uci.ics.hyracks.cli.commands;

import java.io.File;

import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.cli.Session;

public class CreateApplicationCommand extends Command {
    private String appName;

    private File harFile;

    public CreateApplicationCommand(String appName, File harFile) {
        this.appName = appName;
        this.harFile = harFile;
    }

    @Override
    public void run(Session session) throws Exception {
        IHyracksClientConnection hcc = session.getConnection();
        if (hcc == null) {
            throw new RuntimeException("Not connected to Hyracks Cluster Controller");
        }
        System.err.println("Creating application: " + appName + " with har: " + harFile.getAbsolutePath());
        hcc.createApplication(appName, harFile);
    }
}