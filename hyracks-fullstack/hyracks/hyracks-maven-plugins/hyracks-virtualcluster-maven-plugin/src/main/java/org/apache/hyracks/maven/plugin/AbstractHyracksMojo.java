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
package org.apache.hyracks.maven.plugin;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;

public abstract class AbstractHyracksMojo extends AbstractMojo {
    /**
     * @parameter
     */
    protected String jvmOptions;

    protected Process launch(File command, String options, File workingDir) throws MojoExecutionException {
        if (!command.isFile()) {
            throw new MojoExecutionException(command.getAbsolutePath() + " is not an executable program");
        }

        getLog().info("Executing Hyracks command: " + command + " with args [" + options + "]");
        String osName = System.getProperty("os.name");
        try {
            if (osName.startsWith("Windows")) {
                return launchWindowsBatch(command, options);
            } else {
                return launchUnixScript(command, options, workingDir);
            }
        } catch (IOException e) {
            throw new MojoExecutionException("Error executing command: " + command.getAbsolutePath(), e);
        }
    }

    protected Process launchWindowsBatch(File command, String options) throws IOException {
        String[] commandWithOptions = new String[] { "cmd.exe", "/C", command.getAbsolutePath() + " " + options };

        Process proc = Runtime.getRuntime().exec(commandWithOptions);
        dump(proc.getInputStream());
        dump(proc.getErrorStream());
        return proc;
    }

    protected Process launchUnixScript(File command, String options, File workingDir) throws IOException {
        String[] optionsArray = new String[0];
        if (options != null && !options.trim().isEmpty()) {
            optionsArray = options.trim().split("\\s+");
        }
        String[] commandWithOptions = new String[optionsArray.length + 1];
        commandWithOptions[0] = command.getAbsolutePath();
        for (int i = 0; i < optionsArray.length; ++i) {
            commandWithOptions[i + 1] = optionsArray[i];
        }
        ProcessBuilder pb = new ProcessBuilder(commandWithOptions);
        if (jvmOptions != null) {
            pb.environment().put("JAVA_OPTS", jvmOptions);
        }
        pb.directory(workingDir == null ? new File(".") : workingDir);
        Process proc = pb.start();
        dump(proc.getInputStream());
        dump(proc.getErrorStream());
        return proc;
    }

    protected void dump(final InputStream input) {
        final int streamBufferSize = 1000;
        final Reader in = new InputStreamReader(input);
        new Thread(new Runnable() {
            public void run() {
                try {
                    char[] chars = new char[streamBufferSize];
                    int c;
                    while ((c = in.read(chars)) != -1) {
                        if (c > 0) {
                            System.out.print(String.valueOf(chars, 0, c));
                        }
                    }
                } catch (IOException e) {
                }
            }
        }).start();
    }

    protected String makeScriptName(String scriptName) {
        String osName = System.getProperty("os.name");
        String commandExt = osName.startsWith("Windows") ? ".bat" : "";
        return scriptName + commandExt;
    }
}
