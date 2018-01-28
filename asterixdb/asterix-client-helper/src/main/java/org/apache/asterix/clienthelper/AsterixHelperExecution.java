/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.clienthelper;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.asterix.clienthelper.commands.ClientCommand;
import org.apache.asterix.clienthelper.commands.ClientCommand.Command;
import org.apache.asterix.clienthelper.commands.GetClusterStateCommand;
import org.apache.asterix.clienthelper.commands.ShutdownAllCommand;
import org.apache.asterix.clienthelper.commands.ShutdownCommand;
import org.apache.asterix.clienthelper.commands.SleepCommand;
import org.apache.asterix.clienthelper.commands.WaitForClusterCommand;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.ParserProperties;

public class AsterixHelperExecution {

    public static final String ASTERIX_HELPER = "asterixhelper";
    public static final int COMMAND_USAGE_ALIGNMENT = 20;

    protected AsterixHelperExecution() {
    }

    @SuppressWarnings({ "squid:S106", // use of System.err
            "squid:S1166" // rethrow or log exception
    })
    public int execute(String[] argArray) throws IOException {
        Args args = createArgs();
        CmdLineParser parser = createParser(args);
        try {
            parser.parseArgument(argArray);
            if (args.getArguments().isEmpty()) {
                throw new CmdLineException(parser, "No command specified.", null);
            }
            ClientCommand command = getCommand(args);
            if (command == null) {
                throw new CmdLineException(parser, "Unknown command specified: " + args.getArguments().get(0), null);
            } else {
                return command.execute();
            }
        } catch (CmdLineException e) {
            System.err.println(
                    "ERROR: " + e.getMessage() + "\n\n" + "Usage: " + getHelperCommandName() + " [options] <command>");

            printUsageDetails(parser, System.err);
            return 99;
        }
    }

    protected void printUsageDetails(CmdLineParser parser, PrintStream ps) {
        ps.println("\nCommands:");
        printCommandsUsage(ps);
        ps.println("\nOptions:");
        parser.printUsage(ps);
        ps.flush();
    }

    protected String getHelperCommandName() {
        return ASTERIX_HELPER;
    }

    protected void printCommandsUsage(PrintStream out) {
        for (Command command : Command.values()) {
            printCommandUsage(out, command);
        }
    }

    protected void printCommandUsage(PrintStream out, Command command) {
        printCommandUsage(out, command.name(), command.usage());
    }

    protected void printCommandUsage(PrintStream out, String name, String usage) {
        StringBuilder padding = new StringBuilder();
        for (int i = name.length(); i < COMMAND_USAGE_ALIGNMENT; i++) {
            padding.append(' ');
        }
        out.println("  " + name.toLowerCase() + padding.toString() + " : " + usage);
    }

    protected CmdLineParser createParser(Args args) {
        return new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(120));
    }

    protected Args createArgs() {
        return new Args();
    }

    @SuppressWarnings("squid:S1166") // rethrow or log IllegalArgumentException
    protected ClientCommand getCommand(Args args) {

        Command command = Command.valueOfSafe(args.getArguments().get(0));
        if (command == null) {
            return null;
        }
        switch (command) {
            case GET_CLUSTER_STATE:
                return new GetClusterStateCommand(args);
            case WAIT_FOR_CLUSTER:
                return new WaitForClusterCommand(args);
            case SHUTDOWN_CLUSTER:
                return new ShutdownCommand(args);
            case SHUTDOWN_CLUSTER_ALL:
                return new ShutdownAllCommand(args);
            case SLEEP:
                return new SleepCommand(args);
            default:
                throw new IllegalStateException("NYI: " + command);
        }
    }
}
