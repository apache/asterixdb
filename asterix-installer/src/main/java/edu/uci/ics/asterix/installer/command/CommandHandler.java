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

import edu.uci.ics.asterix.installer.command.ICommand.CommandType;

public class CommandHandler {

    public void processCommand(String args[]) throws Exception {
        CommandType cmdType = CommandType.valueOf(args[0].toUpperCase());
        ICommand cmd = null;
        switch (cmdType) {
            case CREATE:
                cmd = new CreateCommand();
                break;
            case ALTER:
                cmd = new AlterCommand();
                break;
            case DELETE:
                cmd = new DeleteCommand();
                break;
            case DESCRIBE:
                cmd = new DescribeCommand();
                break;
            case BACKUP:
                cmd = new BackupCommand();
                break;
            case RESTORE:
                cmd = new RestoreCommand();
                break;
            case START:
                cmd = new StartCommand();
                break;
            case STOP:
                cmd = new StopCommand();
                break;
            case VALIDATE:
                cmd = new ValidateCommand();
                break;
            case CONFIGURE:
                cmd = new ConfigureCommand();
                break;
            case LOG:
                cmd = new LogCommand();
                break;
            case SHUTDOWN:
                cmd = new ShutdownCommand();
                break;
            case HELP:
                cmd = new HelpCommand();
                break;
        }
        cmd.execute(args);
    }
}
