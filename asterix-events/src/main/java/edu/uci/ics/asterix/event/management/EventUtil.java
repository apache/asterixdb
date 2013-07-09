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

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import edu.uci.ics.asterix.event.driver.EventDriver;
import edu.uci.ics.asterix.event.management.ValueType.Type;
import edu.uci.ics.asterix.event.schema.cluster.Cluster;
import edu.uci.ics.asterix.event.schema.cluster.Node;
import edu.uci.ics.asterix.event.schema.event.Event;
import edu.uci.ics.asterix.event.schema.event.Events;
import edu.uci.ics.asterix.event.schema.pattern.Pattern;

public class EventUtil {

	public static final String EVENTS_DIR = "events";
	public static final String CLUSTER_CONF = "config/cluster.xml";
	public static final String PATTERN_CONF = "config/pattern.xml";
	public static final DateFormat dateFormat = new SimpleDateFormat(
			"yyyy/MM/dd HH:mm:ss");
	public static final String NC_JAVA_OPTS = "nc.java.opts";
	public static final String CC_JAVA_OPTS = "cc.java.opts";

	private static final String IP_LOCATION = "IP_LOCATION";
	private static final String CLUSTER_ENV = "ENV";
	private static final String SCRIPT = "SCRIPT";
	private static final String ARGS = "ARGS";
	private static final String EXECUTE_SCRIPT = "events/execute.sh";
	private static final String LOCALHOST = "localhost";
	private static final String LOCALHOST_IP = "127.0.0.1";

	public static Cluster getCluster(String clusterConfigurationPath)
			throws JAXBException {
		File file = new File(clusterConfigurationPath);
		JAXBContext ctx = JAXBContext.newInstance(Cluster.class);
		Unmarshaller unmarshaller = ctx.createUnmarshaller();
		Cluster cluster = (Cluster) unmarshaller.unmarshal(file);
		if (cluster.getMasterNode().getClusterIp().equals(LOCALHOST)) {
			cluster.getMasterNode().setClusterIp(LOCALHOST_IP);
		}
		for (Node node : cluster.getNode()) {
			if (node.getClusterIp().equals(LOCALHOST)) {
				node.setClusterIp(LOCALHOST_IP);
			}
		}
		return cluster;
	}

	public static long parseTimeInterval(ValueType v, String unit)
			throws IllegalArgumentException {
		int val = 0;
		switch (v.getType()) {
		case ABS:
			val = Integer.parseInt(v.getAbsoluteValue());
			break;
		case RANDOM_MIN_MAX:
			val = Randomizer.getInstance().getRandomInt(v.getMin(), v.getMax());
			break;
		case RANDOM_RANGE:
			String[] values = v.getRangeSet();
			val = Integer.parseInt(values[Randomizer.getInstance()
					.getRandomInt(0, values.length - 1)]);
			break;
		}
		return computeInterval(val, unit);
	}

	public static long parseTimeInterval(String v, String unit)
			throws IllegalArgumentException {
		int value = Integer.parseInt(v);
		return computeInterval(value, unit);
	}

	private static long computeInterval(int val, String unit) {
		int vmult = 1;
		if ("hr".equalsIgnoreCase(unit)) {
			vmult = 3600 * 1000;
		} else if ("min".equalsIgnoreCase(unit)) {
			vmult = 60 * 1000;
		} else if ("sec".equalsIgnoreCase(unit)) {
			vmult = 1000;
		} else
			throw new IllegalArgumentException(
					" invalid unit value specified for frequency (hr,min,sec)");
		return val * vmult;

	}

	public static Event getEvent(Pattern pattern, Events events) {
		for (Event event : events.getEvent()) {
			if (event.getType().equals(pattern.getEvent().getType())) {
				return event;
			}
		}
		throw new IllegalArgumentException(" Unknown event type"
				+ pattern.getEvent().getType());
	}

	public static Node getEventLocation(Pattern pattern,
			List<Node> candidateLocations, Cluster cluster) {
		ValueType value = new ValueType(pattern.getEvent().getNodeid()
				.getValue());
		Node location = null;
		Type vtype = value.getType();

		switch (vtype) {
		case ABS:
			location = getNodeFromId(value.getAbsoluteValue(), cluster);
			break;
		case RANDOM_RANGE:
			int nodeIndex = Randomizer.getInstance().getRandomInt(0,
					candidateLocations.size() - 1);
			location = candidateLocations.get(nodeIndex);
			break;
		case RANDOM_MIN_MAX:
			throw new IllegalStateException(
					" Canont configure a min max value range for location");
		}
		return location;

	}

	public static List<Node> getCandidateLocations(Pattern pattern,
			Cluster cluster) {
		ValueType value = new ValueType(pattern.getEvent().getNodeid()
				.getValue());
		List<Node> candidateList = new ArrayList<Node>();
		switch (value.getType()) {
		case ABS:
			candidateList.add(getNodeFromId(value.getAbsoluteValue(), cluster));
			break;
		case RANDOM_RANGE:
			boolean anyOption = false;
			String[] values = value.getRangeSet();
			for (String v : values) {
				if (v.equalsIgnoreCase("ANY")) {
					anyOption = true;
				}
			}
			if (anyOption) {
				for (Node node : cluster.getNode()) {
					candidateList.add(node);
				}
			} else {
				boolean found = false;
				for (String v : values) {
					for (Node node : cluster.getNode()) {
						if (node.getId().equals(v)) {
							candidateList.add(node);
							found = true;
							break;
						}
					}
					if (!found) {
						throw new IllegalStateException("Unknonw nodeId : " + v);
					}
					found = false;
				}

			}
			String[] excluded = value.getRangeExcluded();
			if (excluded != null && excluded.length > 0) {
				List<Node> markedForRemoval = new ArrayList<Node>();
				for (String exclusion : excluded) {
					for (Node node : candidateList) {
						if (node.getId().equals(exclusion)) {
							markedForRemoval.add(node);
						}
					}
				}
				candidateList.removeAll(markedForRemoval);
			}
			break;
		case RANDOM_MIN_MAX:
			throw new IllegalStateException(
					" Invalid value configured for location");
		}
		return candidateList;
	}

	private static Node getNodeFromId(String nodeid, Cluster cluster) {
		if (nodeid.equals(EventDriver.CLIENT_NODE.getId())) {
			return EventDriver.CLIENT_NODE;
		}

		if (nodeid.equals(cluster.getMasterNode().getId())) {
			String logDir = cluster.getMasterNode().getLogDir() == null ? cluster
					.getLogDir()
					: cluster.getMasterNode().getLogDir();
			String javaHome = cluster.getMasterNode().getJavaHome() == null ? cluster
					.getJavaHome()
					: cluster.getMasterNode().getJavaHome();
			return new Node(cluster.getMasterNode().getId(), cluster
					.getMasterNode().getClusterIp(), javaHome, logDir, null,
					null, null);
		}

		List<Node> nodeList = cluster.getNode();
		for (Node node : nodeList) {
			if (node.getId().equals(nodeid)) {
				return node;
			}
		}
		StringBuffer buffer = new StringBuffer();
		buffer.append(EventDriver.CLIENT_NODE.getId() + ",");
		buffer.append(cluster.getMasterNode().getId() + ",");
		for (Node v : cluster.getNode()) {
			buffer.append(v.getId() + ",");
		}
		buffer.deleteCharAt(buffer.length() - 1);
		throw new IllegalArgumentException("Unknown node id :" + nodeid
				+ " valid ids:" + buffer);
	}

	public static void executeEventScript(Node node, String script,
			List<String> args, Cluster cluster) throws IOException,
			InterruptedException {
		List<String> pargs = new ArrayList<String>();
		pargs.add("/bin/bash");
		pargs.add(EventDriver.getEventsDir() + "/" + EXECUTE_SCRIPT);
		StringBuffer argBuffer = new StringBuffer();
		String env = EventDriver.getStringifiedEnv(cluster) + " " + IP_LOCATION
				+ "=" + node.getClusterIp();
		if (args != null) {
			for (String arg : args) {
				argBuffer.append(arg + " ");
			}
		}
		ProcessBuilder pb = new ProcessBuilder(pargs);
		pb.environment().putAll(EventDriver.getEnvironment());
		pb.environment().put(IP_LOCATION, node.getClusterIp());
		pb.environment().put(CLUSTER_ENV, env);
		pb.environment().put(SCRIPT, script);
		pb.environment().put(ARGS, argBuffer.toString());
		pb.start();
	}

	public static void executeLocalScript(Node node, String script,
			List<String> args) throws IOException, InterruptedException {
		List<String> pargs = new ArrayList<String>();
		pargs.add("/bin/bash");
		pargs.add(script);
		if (args != null) {
			pargs.addAll(args);
		}
		ProcessBuilder pb = new ProcessBuilder(pargs);
		pb.environment().putAll(EventDriver.getEnvironment());
		pb.environment().put(IP_LOCATION, node.getClusterIp());
		pb.start();
	}

	public static List<String> getEventArgs(Pattern pattern) {
		List<String> pargs = new ArrayList<String>();
		if (pattern.getEvent().getPargs() == null) {
			return pargs;
		}
		String[] args = pattern.getEvent().getPargs().split(" ");
		for (String arg : args) {
			pargs.add(arg.trim());
		}
		return pargs;
	}

}
