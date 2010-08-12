package edu.uci.ics.hyracks.cli.commands;

import java.io.File;

import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.cli.CLI;

public class CreateApplicationCommand extends Command {
    private String appName;

    private File harFile;

    public CreateApplicationCommand(String appName, File harFile) {
        this.appName = appName;
        this.harFile = harFile;
    }

    @Override
    public void run(CLI cli) throws Exception {
        IHyracksClientConnection hcc = cli.getConnection();
        if (hcc == null) {
            throw new RuntimeException("Not connected to Hyracks Cluster Controller");
        }
        System.err.println("Creating application: " + appName + " with har: " + harFile.getAbsolutePath());
        hcc.createApplication(appName, harFile);
    }
}