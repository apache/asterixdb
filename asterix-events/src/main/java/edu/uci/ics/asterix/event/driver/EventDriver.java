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
package edu.uci.ics.asterix.event.driver;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.kohsuke.args4j.CmdLineParser;

import edu.uci.ics.asterix.event.management.DefaultOutputHandler;
import edu.uci.ics.asterix.event.management.EventUtil;
import edu.uci.ics.asterix.event.management.EventrixClient;
import edu.uci.ics.asterix.event.management.IOutputHandler;
import edu.uci.ics.asterix.event.management.Randomizer;
import edu.uci.ics.asterix.event.schema.cluster.Cluster;
import edu.uci.ics.asterix.event.schema.cluster.Node;
import edu.uci.ics.asterix.event.schema.cluster.Property;
import edu.uci.ics.asterix.event.schema.event.Events;
import edu.uci.ics.asterix.event.schema.pattern.Patterns;

public class EventDriver {

    public static final String CLIENT_NODE_ID = "client_node";
    public static final Node CLIENT_NODE = new Node(CLIENT_NODE_ID, "127.0.0.1", null, null, null, null, null);

    private static String eventsDir;
    private static Events events;
    private static Map<String, String> env = new HashMap<String, String>();
    private static String scriptDirSuffix;

    public static String getEventsDir() {
        return eventsDir;
    }

    public static Events getEvents() {
        return events;
    }

    public static Map<String, String> getEnvironment() {
        return env;
    }

    public static String getStringifiedEnv(Cluster cluster) {
        StringBuffer buffer = new StringBuffer();
        for (Property p : cluster.getEnv().getProperty()) {
            buffer.append(p.getKey() + "=" + p.getValue() + " ");
        }
        return buffer.toString();
    }

    public static Cluster initializeCluster(String path) throws JAXBException, IOException {
        Cluster cluster = EventUtil.getCluster(path);
        for (Property p : cluster.getEnv().getProperty()) {
            env.put(p.getKey(), p.getValue());
        }
        return cluster;
    }

    public static Patterns initializePatterns(String path) throws JAXBException, IOException {
        File file = new File(path);
        JAXBContext ctx = JAXBContext.newInstance(Patterns.class);
        Unmarshaller unmarshaller = ctx.createUnmarshaller();
        return (Patterns) unmarshaller.unmarshal(file);
    }

    private static void initialize(EventConfig eventConfig) throws IOException, JAXBException {

    }

    public static EventrixClient getClient(String eventsDir, Cluster cluster, boolean dryRun) throws Exception {
        return new EventrixClient(eventsDir, cluster, dryRun, new DefaultOutputHandler());
    }

    public static EventrixClient getClient(String eventsDir, Cluster cluster, boolean dryRun,
            IOutputHandler outputHandler) throws Exception {
        return new EventrixClient(eventsDir, cluster, dryRun, outputHandler);
    }

    public static void main(String[] args) throws Exception {
        String eventsHome = System.getenv("EVENT_HOME");
        if (eventsHome == null) {
            throw new IllegalStateException("EVENT_HOME is not set");
        }
        eventsDir = eventsHome + File.separator + EventUtil.EVENTS_DIR;
        EventConfig eventConfig = new EventConfig();
        CmdLineParser parser = new CmdLineParser(eventConfig);
        try {
            parser.parseArgument(args);
            if (eventConfig.help) {
                parser.printUsage(System.out);
            }
            if (eventConfig.seed > 0) {
                Randomizer.getInstance(eventConfig.seed);
            }
            Cluster cluster = initializeCluster(eventConfig.clusterPath);
            Patterns patterns = initializePatterns(eventConfig.patternPath);
            initialize(eventConfig);

            if (!eventConfig.dryRun) {
                prepare(cluster);
            }
            EventrixClient client = new EventrixClient(eventsDir, cluster, eventConfig.dryRun,
                    new DefaultOutputHandler());
            client.submit(patterns);
            if (!eventConfig.dryRun) {
                cleanup(cluster);
            }
        } catch (Exception e) {
            e.printStackTrace();
            parser.printUsage(System.err);
        }
    }

    private static void prepare(Cluster cluster) throws IOException, InterruptedException {

        scriptDirSuffix = "" + System.nanoTime();
        List<String> args = new ArrayList<String>();
        args.add(scriptDirSuffix);
        Node clientNode = new Node();
        clientNode.setId("client");
        clientNode.setClusterIp("127.0.0.1");
        for (Node node : cluster.getNode()) {
            args.add(node.getClusterIp());
        }
        EventUtil.executeLocalScript(clientNode, eventsDir + "/" + "events" + "/" + "prepare.sh", args);
    }

    private static void cleanup(Cluster cluster) throws IOException, InterruptedException {
        List<String> args = new ArrayList<String>();
        args.add(scriptDirSuffix);
        Node clientNode = new Node();
        clientNode.setId("client");
        clientNode.setClusterIp("127.0.0.1");
        for (Node node : cluster.getNode()) {
            args.add(node.getClusterIp());
        }
        EventUtil.executeLocalScript(clientNode, eventsDir + "/" + "events" + "/" + "cleanup.sh", args);
    }
}
