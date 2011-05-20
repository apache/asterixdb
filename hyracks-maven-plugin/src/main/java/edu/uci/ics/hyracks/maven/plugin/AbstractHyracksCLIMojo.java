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
package edu.uci.ics.hyracks.maven.plugin;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;

import edu.uci.ics.hyracks.cli.CommandExecutor;
import edu.uci.ics.hyracks.cli.Session;

public abstract class AbstractHyracksCLIMojo extends AbstractMojo {
    /**
     * @parameter
     * @required
     */
    private String ccHost;

    /**
     * @parameter
     */
    private int ccPort;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        try {
            Session session = new Session();
            CommandExecutor.execute(session, createConnectCommand());
            CommandExecutor.execute(session, getCommands());
        } catch (Exception e) {
            throw new MojoExecutionException(e.getMessage());
        }
    }

    private String createConnectCommand() {
        return "connect to \"" + ccHost + (ccPort == 0 ? "" : (":" + ccPort)) + "\";";
    }

    protected abstract String getCommands();
}