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
package edu.uci.ics.asterix.event.management;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;

import edu.uci.ics.asterix.event.driver.EventDriver;
import edu.uci.ics.asterix.event.schema.cluster.Cluster;
import edu.uci.ics.asterix.event.schema.cluster.Node;
import edu.uci.ics.asterix.event.schema.cluster.Property;
import edu.uci.ics.asterix.event.schema.pattern.Pattern;

public class EventExecutor {

    public static final String EVENTS_DIR = "events";
    private static final String EXECUTE_SCRIPT = "execute.sh";
    private static final String IP_LOCATION = "IP_LOCATION";
    private static final String CLUSTER_ENV = "ENV";
    private static final String SCRIPT = "SCRIPT";
    private static final String ARGS = "ARGS";
    private static final String DAEMON = "DAEMON";

    public void executeEvent(Node node, String script, List<String> args, boolean isDaemon, Cluster cluster,
            Pattern pattern, IOutputHandler outputHandler, EventrixClient client) throws IOException {
        List<String> pargs = new ArrayList<String>();
        pargs.add("/bin/bash");
        pargs.add(client.getEventsDir() + File.separator + "scripts" + File.separator + EXECUTE_SCRIPT);
        StringBuffer envBuffer = new StringBuffer(IP_LOCATION + "=" + node.getClusterIp() + " ");
        boolean isMasterNode = node.getId().equals(cluster.getMasterNode().getId());

        if (!node.getId().equals(EventDriver.CLIENT_NODE_ID) && cluster.getEnv() != null) {
            for (Property p : cluster.getEnv().getProperty()) {
                if (p.getKey().equals("JAVA_HOME")) {
                    String val = node.getJavaHome() == null ? p.getValue() : node.getJavaHome();
                    envBuffer.append(p.getKey() + "=" + val + " ");
                } else if (p.getKey().equals(EventUtil.NC_JAVA_OPTS)) {
                    if (!isMasterNode) {
                        StringBuilder builder = new StringBuilder();
                        builder.append("\"");
                        String javaOpts = p.getValue();
                        if (javaOpts != null) {
                            builder.append(javaOpts);
                        }
                        builder.append("\"");
                        envBuffer.append("JAVA_OPTS" + "=" + builder + " ");
                    }
                } else if (p.getKey().equals(EventUtil.CC_JAVA_OPTS)) {
                    if (isMasterNode) {
                        StringBuilder builder = new StringBuilder();
                        builder.append("\"");
                        String javaOpts = p.getValue();
                        if (javaOpts != null) {
                            builder.append(javaOpts);
                        }
                        builder.append("\"");
                        envBuffer.append("JAVA_OPTS" + "=" + builder + " ");
                    }
                } else if (p.getKey().equals("LOG_DIR")) {
                    String val = node.getLogDir() == null ? p.getValue() : node.getLogDir();
                    envBuffer.append(p.getKey() + "=" + val + " ");
                } else {
                    envBuffer.append(p.getKey() + "=" + p.getValue() + " ");
                }

            }
            pargs.add(cluster.getUsername() == null ? System.getProperty("user.name") : cluster.getUsername());
        }

        StringBuffer argBuffer = new StringBuffer();
        if (args != null && args.size() > 0) {
            for (String arg : args) {
                argBuffer.append(arg + " ");
            }
        }

        ProcessBuilder pb = new ProcessBuilder(pargs);
        pb.environment().put(IP_LOCATION, node.getClusterIp());
        pb.environment().put(CLUSTER_ENV, envBuffer.toString());
        pb.environment().put(SCRIPT, script);
        pb.environment().put(ARGS, argBuffer.toString());
        pb.environment().put(DAEMON, isDaemon ? "true" : "false");

        Process p = pb.start();
        if (!isDaemon) {
            BufferedInputStream bis = new BufferedInputStream(p.getInputStream());
            StringWriter writer = new StringWriter();
            IOUtils.copy(bis, writer, "UTF-8");
            String result = writer.getBuffer().toString();
            OutputAnalysis analysis = outputHandler.reportEventOutput(pattern.getEvent(), result);
            if (!analysis.isExpected()) {
                throw new IOException(analysis.getErrorMessage() + result);
            }
        }
    }
}
