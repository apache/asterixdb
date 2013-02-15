/*
 * Copyright 2009-2012 by The Regents of the University of California
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import edu.uci.ics.asterix.event.driver.EventDriver;
import edu.uci.ics.asterix.event.schema.cluster.Cluster;
import edu.uci.ics.asterix.event.schema.cluster.Node;
import edu.uci.ics.asterix.event.schema.pattern.Event;
import edu.uci.ics.asterix.event.schema.pattern.Nodeid;
import edu.uci.ics.asterix.event.schema.pattern.Pattern;
import edu.uci.ics.asterix.event.schema.pattern.Patterns;
import edu.uci.ics.asterix.event.schema.pattern.Value;

public class EventrixClient {

    private static final Logger LOGGER = Logger.getLogger(EventrixClient.class.getName());

    private EventTask[] tasks;
    private boolean dryRun = false;
    private LinkedBlockingQueue<EventTaskReport> msgInbox = new LinkedBlockingQueue<EventTaskReport>();
    private AtomicInteger pendingTasks = new AtomicInteger(0);
    private final Cluster cluster;
    private IPatternListener listener;
    private IOutputHandler outputHandler;

    public EventrixClient(Cluster cluster, boolean dryRun, IOutputHandler outputHandler) throws Exception {
        this.cluster = cluster;
        this.dryRun = dryRun;
        this.outputHandler = outputHandler;
        if (!dryRun) {
            initializeCluster();
        }
    }

    public void submit(Patterns patterns) throws Exception {
        initTasks(patterns);
        try {
            waitForCompletion();
        } catch (InterruptedException ie) {
            LOGGER.info("Interrupted exception :" + ie);
        } catch (Exception e) {
            throw e;
        }

    }

    public void submit(Patterns patterns, IPatternListener listener) throws Exception {
        this.listener = listener;
        initTasks(patterns);
    }

    private void initTasks(Patterns patterns) {
        tasks = new EventTask[patterns.getPattern().size()];
        pendingTasks.set(tasks.length);
        int index = 0;
        for (Pattern pattern : patterns.getPattern()) {
            tasks[index] = new EventTask(pattern, this);
            tasks[index].start();
            index++;
        }
    }

    public Cluster getCluster() {
        return cluster;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public void notifyCompletion(EventTaskReport report) {

        if (report.isSuccess()) {
            pendingTasks.decrementAndGet();
            if (listener != null) {
                listener.eventCompleted(report);
                if (pendingTasks.get() == 0) {
                    listener.jobCompleted();
                }
            } else {
                try {
                    msgInbox.put(report);
                } catch (InterruptedException e) {
                }
            }
        } else {
            for (EventTask t : tasks) {
                if (t.getState() == EventTask.State.INITIALIZED || t.getState() == EventTask.State.IN_PROGRESS) {
                    t.cancel();
                }
            }
            if (listener != null) {
                listener.jobFailed(report);
            } else {
                try {
                    msgInbox.put(report);
                } catch (InterruptedException e) {
                }
            }
        }
    }

    public void notifyFailure(EventTaskReport report) {

    }

    private void waitForCompletion() throws Exception {
        while (true) {
            EventTaskReport report = msgInbox.take();
            if (report.isSuccess()) {
                if (pendingTasks.get() == 0) {
                    break;
                }
            } else {
                throw new RuntimeException(report.getException().getMessage());
            }
        }
    }

    private void initializeCluster() throws Exception {
        Patterns patterns = initPattern();
        submit(patterns);
    }

    private Patterns initPattern() {
        Nodeid nodeid = new Nodeid(new Value(null, EventDriver.CLIENT_NODE.getId()));
        List<Pattern> patternList = new ArrayList<Pattern>();
        String workingDir = cluster.getWorkingDir().getDir();
        File eventsPkg = new File(EventDriver.getHomeDir() + File.separator + "events.pkg.tar");
        if (!eventsPkg.exists()) {
            throw new IllegalStateException(" Events package does not exist (" + eventsPkg.getAbsolutePath() + ")");
        }

        String username = cluster.getUsername() == null ? System.getProperty("user.name") : cluster.getUsername();
        patternList.add(getFileTransferPattern(username, eventsPkg.getAbsolutePath(), nodeid, cluster.getMasterNode()
                .getIp(), workingDir, true));

        if (!cluster.getWorkingDir().isNFS()) {
            for (Node node : cluster.getNode()) {
                patternList.add(getFileTransferPattern(username, eventsPkg.getAbsolutePath(), nodeid, node.getIp(),
                        workingDir, true));
            }
        }
        Patterns patterns = new Patterns(patternList);
        return patterns;
    }

    private Pattern getFileTransferPattern(String username, String src, Nodeid srcNode, String destNodeIp,
            String destDir, boolean unpack) {
        String pargs = username + " " + src + " " + destNodeIp + " " + destDir + " " + "unpack";
        Event event = new Event("file_transfer", srcNode, pargs);
        return new Pattern(null, 1, null, event);
    }

    public IOutputHandler getErrorHandler() {
        return outputHandler;
    }
}
