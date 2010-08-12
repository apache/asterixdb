package edu.uci.ics.hyracks.cli.commands;

import edu.uci.ics.hyracks.cli.CLI;

public class DisconnectCommand extends Command {
    @Override
    public void run(CLI cli) throws Exception {
        System.err.println("Disconnecting...");
        cli.setConnection(null);
    }
}