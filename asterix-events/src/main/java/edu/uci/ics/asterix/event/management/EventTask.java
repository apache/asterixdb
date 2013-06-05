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
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.log4j.Logger;

import edu.uci.ics.asterix.event.driver.EventDriver;
import edu.uci.ics.asterix.event.schema.cluster.Node;
import edu.uci.ics.asterix.event.schema.event.Event;
import edu.uci.ics.asterix.event.schema.pattern.Pattern;
import edu.uci.ics.asterix.event.schema.pattern.Period;

public class EventTask extends TimerTask {

	public static enum State {
		INITIALIZED, IN_PROGRESS, COMPLETED, FAILED
	}

	private static final Logger logger = Logger.getLogger(EventTask.class
			.getName());

	private Pattern pattern;
	private Event event;
	private long interval = 0;
	private long initialDelay = 0;
	private int maxOccurs = Integer.MAX_VALUE;
	private int occurrenceCount = 0;
	private Timer timer;
	private String taskScript;
	private Node location;
	private List<String> taskArgs;
	private EventrixClient client;
	private List<Node> candidateLocations;
	private boolean dynamicLocation = false;
	private boolean reuseLocation = false;
	private State state;

	

	public EventTask(Pattern pattern, EventrixClient client) {
		this.pattern = pattern;
		this.client = client;
		Period period = pattern.getPeriod();
		if (period != null && period.getAbsvalue() != null) {
			this.interval = EventUtil.parseTimeInterval(period.getAbsvalue(),
					period.getUnit());
		}
		if (pattern.getDelay() != null) {
			this.initialDelay = EventUtil.parseTimeInterval(new ValueType(
					pattern.getDelay().getValue()), pattern.getDelay()
					.getUnit());
		}
		if (pattern.getMaxOccurs() != null) {
			this.maxOccurs = pattern.getMaxOccurs();
		}
		this.timer = new Timer();
		taskArgs = EventUtil.getEventArgs(pattern);
		candidateLocations = EventUtil.getCandidateLocations(pattern,
				client.getCluster());
		if (pattern.getEvent().getNodeid().getValue().getRandom() != null
				&& period != null && maxOccurs > 1) {
			dynamicLocation = true;
			reuseLocation = pattern.getEvent().getNodeid().getValue()
					.getRandom().getRange().isReuse();
		} else {
			location = EventUtil.getEventLocation(pattern, candidateLocations,
					client.getCluster());
		}
		String scriptsDir;
		if (location.getId().equals(EventDriver.CLIENT_NODE_ID)) {
			scriptsDir = client.getEventsDir() + File.separator + "events";
		} else {
			scriptsDir = client.getCluster().getWorkingDir().getDir()
					+ File.separator + "eventrix" + File.separator + "events";
		}
		event = EventUtil.getEvent(pattern, client.getEvents());
		taskScript = scriptsDir + File.separator + event.getScript();
		state = State.INITIALIZED;
	}

	public void start() {
		if (interval > 0) {
			timer.schedule(this, initialDelay, interval);
		} else {
			timer.schedule(this, initialDelay);
		}
	}

	@Override
	public void run() {
		if (candidateLocations.size() == 0) {
			timer.cancel();
			client.notifyCompletion(new EventTaskReport(this));
		} else {
			if (dynamicLocation) {
				location = EventUtil.getEventLocation(pattern,
						candidateLocations, client.getCluster());
				if (!reuseLocation) {
					candidateLocations.remove(location);
				}
			}

			logger.debug(EventUtil.dateFormat.format(new Date()) + " "
					+ "EVENT " + pattern.getEvent().getType().toUpperCase()
					+ " at " + location.getId().toUpperCase());
			try {
				if (!client.isDryRun()) {
					new EventExecutor().executeEvent(location, taskScript,
							taskArgs, event.isDaemon(), client.getCluster(),
							pattern, client.getErrorHandler(), client);
				}
				occurrenceCount++;
				if (occurrenceCount >= maxOccurs) {
					timer.cancel();
					client.notifyCompletion(new EventTaskReport(this));
				}
			} catch (IOException ioe) {
				timer.cancel();
				client.notifyCompletion(new EventTaskReport(this, false, ioe));
			}
		}

	}

	public Node getLocation() {
		return location;
	}

	public long getInterval() {
		return interval;
	}

	public long getInitialDelay() {
		return initialDelay;
	}

	public Pattern getPattern() {
		return pattern;
	}

	public State getState() {
		return state;
	}

}
