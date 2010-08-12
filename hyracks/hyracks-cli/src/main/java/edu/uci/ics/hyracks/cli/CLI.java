/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.cli;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import jline.ConsoleReader;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.cli.commands.Command;

public class CLI {
    private static final String HYRACKS_PROMPT = "hyracks> ";
    private static final String HYRAX_CONTINUE_PROMPT = "> ";
    private final ConsoleReader reader;
    private IHyracksClientConnection connection;

    public CLI(String[] args) throws IOException {
        reader = new ConsoleReader();
        connection = null;
    }

    public void run() throws IOException {
        boolean eof = false;
        while (true) {
            String prompt = HYRACKS_PROMPT;
            StringBuffer command = new StringBuffer();
            while (true) {
                String line = reader.readLine(prompt);
                if (line == null) {
                    eof = true;
                    break;
                }
                prompt = HYRAX_CONTINUE_PROMPT;
                line = line.trim();
                command.append(line);
                if ("".equals(line)) {
                    break;
                }
                if (line.endsWith(";")) {
                    break;
                }
            }
            if (eof) {
                break;
            }
            try {
                execute(command);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void execute(StringBuffer command) throws Exception {
        CLIParser parser = new CLIParser(new StringReader(command.toString()));
        List<Command> cmds = parser.Commands();
        for (Command cmd : cmds) {
            cmd.run(this);
        }
    }

    public void setConnection(IHyracksClientConnection connection) {
        this.connection = connection;
    }

    public IHyracksClientConnection getConnection() {
        return connection;
    }
}