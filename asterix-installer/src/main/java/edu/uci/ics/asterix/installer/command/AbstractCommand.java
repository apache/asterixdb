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
package edu.uci.ics.asterix.installer.command;

import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineParser;

public abstract class AbstractCommand implements ICommand {

    protected static final Logger LOGGER = Logger.getLogger(AbstractCommand.class.getName());

    protected CommandConfig config;

    protected String usageDescription;

    public void execute(String[] args) throws Exception {
        String[] cmdArgs = new String[args.length - 1];
        System.arraycopy(args, 1, cmdArgs, 0, cmdArgs.length);
        config = getCommandConfig();
        CmdLineParser parser = new CmdLineParser(config);
        parser.parseArgument(cmdArgs);
        execCommand();
    }

    abstract protected void execCommand() throws Exception;

    abstract protected String getUsageDescription();

    abstract protected CommandConfig getCommandConfig();
}
