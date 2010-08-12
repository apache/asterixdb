package edu.uci.ics.hyracks.cli.commands;

import edu.uci.ics.hyracks.cli.CLI;

public abstract class Command {
    public abstract void run(CLI cli) throws Exception;
}