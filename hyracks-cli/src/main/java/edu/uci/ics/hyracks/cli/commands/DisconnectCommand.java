package edu.uci.ics.hyracks.cli.commands;

import edu.uci.ics.hyracks.cli.Session;

public class DisconnectCommand extends Command {
    @Override
    public void run(Session session) throws Exception {
        System.err.println("Disconnecting...");
        session.setConnection(null);
    }
}