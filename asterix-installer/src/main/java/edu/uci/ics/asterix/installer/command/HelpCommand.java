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

import org.kohsuke.args4j.Option;

public class HelpCommand extends AbstractCommand {

    @Override
    protected void execCommand() throws Exception {
        HelpConfig helpConfig = (HelpConfig) config;
        String command = helpConfig.command;
        CommandType cmdType = CommandType.valueOf(command.toUpperCase());
        String helpMessage = null;
        switch (cmdType) {
            case CREATE:
                helpMessage = new CreateCommand().getUsageDescription();
                break;
            case CONFIGURE:
                helpMessage = new ConfigureCommand().getUsageDescription();
                break;
            case DELETE:
                helpMessage = new DeleteCommand().getUsageDescription();
                break;
            case DESCRIBE:
                helpMessage = new DescribeCommand().getUsageDescription();
                break;
            case RESTORE:
                helpMessage = new RestoreCommand().getUsageDescription();
                break;
            case START:
                helpMessage = new StartCommand().getUsageDescription();
                break;
            case SHUTDOWN:
                helpMessage = new ShutdownCommand().getUsageDescription();
                break;
            case BACKUP:
                helpMessage = new BackupCommand().getUsageDescription();
                break;
            case STOP:
                helpMessage = new StopCommand().getUsageDescription();
                break;
            case VALIDATE:
                helpMessage = new ValidateCommand().getUsageDescription();
                break;
            case ALTER:
                helpMessage = new AlterCommand().getUsageDescription();
                break;
            case LOG:
                helpMessage = new LogCommand().getUsageDescription();
                break;
            default:
                helpMessage = "Unknown command " + command;
        }

        System.out.println(helpMessage);
    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new HelpConfig();
    }

    @Override
    protected String getUsageDescription() {
        return "\nAlter the instance's configuration settings."
                + "\nPrior to running this command, the instance is required to be INACTIVE state."
                + "\n\nAvailable arguments/options" + "\n-n name of the ASTERIX instance"
                + "\n-conf path to the ASTERIX configuration file.";
    }

}

class HelpConfig extends CommandConfig {

    @Option(name = "-cmd", required = true, usage = "Name of Asterix Instance")
    public String command;

}
