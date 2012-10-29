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

import java.io.File;
import java.io.PrintWriter;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;

public abstract class AbstractHyracksCLIMojo extends AbstractHyracksMojo {
    private static final String HYRACKS_CLI_SCRIPT = "bin" + File.separator + "hyrackscli";

    /**
     * @parameter
     * @required
     */
    protected File hyracksCLIHome;

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
        StringBuilder buffer = new StringBuilder();
        buffer.append(createConnectCommand());
        buffer.append('\n');
        buffer.append(getCommands());
        final Process proc = launch(new File(hyracksCLIHome, makeScriptName(HYRACKS_CLI_SCRIPT)), null, null);
        try {
            PrintWriter out = new PrintWriter(proc.getOutputStream());
            out.println(buffer.toString());
            out.close();
            proc.waitFor();
        } catch (Exception e) {
            throw new MojoExecutionException(e.getMessage());
        }
    }

    private String createConnectCommand() {
        return "connect to \"" + ccHost + (ccPort == 0 ? "" : (":" + ccPort)) + "\";";
    }

    protected abstract String getCommands();
}