package edu.uci.ics.hyracks.cli.commands;

import edu.uci.ics.hyracks.cli.Session;

public abstract class Command {
    public abstract void run(Session session) throws Exception;
}