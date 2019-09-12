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
package org.apache.hyracks.client.stats.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.com.job.profiling.counters.Counter;
import org.apache.hyracks.api.job.profiling.counters.ICounter;
import org.apache.hyracks.client.stats.AggregateCounter;
import org.apache.hyracks.client.stats.Counters;
import org.apache.hyracks.client.stats.IClusterCounterContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author yingyib
 */
public class ClientCounterContext implements IClusterCounterContext {
    private static String[] RESET_COUNTERS =
            { Counters.NETWORK_IO_READ, Counters.NETWORK_IO_WRITE, Counters.MEMORY_USAGE, Counters.MEMORY_MAX,
                    Counters.DISK_READ, Counters.DISK_WRITE, Counters.NUM_PROCESSOR };
    private static String[] AGG_COUNTERS = { Counters.SYSTEM_LOAD };
    private static int UPDATE_INTERVAL = 10000;

    private final Map<String, Counter> counterMap = new HashMap<String, Counter>();
    private final String baseURL;
    private final List<String> slaveMachines = new ArrayList<String>();
    private boolean stopped = false;
    private Thread updateThread = new UpdateThread();

    public ClientCounterContext(String hostName, int restPort, Collection<String> slaveMachines) {
        this.baseURL = "http://" + hostName + ":" + restPort + "/rest/nodes/";
        this.slaveMachines.addAll(slaveMachines);
        for (String restCounterName : RESET_COUNTERS) {
            counterMap.put(restCounterName, new AggregateCounter(restCounterName));
        }
        for (String aggCounterName : AGG_COUNTERS) {
            counterMap.put(aggCounterName, new AggregateCounter(aggCounterName));
        }
        for (String slave : slaveMachines) {
            for (String restCounterName : RESET_COUNTERS) {
                counterMap.put(slave + "$" + restCounterName, new AggregateCounter(restCounterName));
            }
            for (String aggCounterName : AGG_COUNTERS) {
                counterMap.put(slave + "$" + aggCounterName, new AggregateCounter(aggCounterName));
            }
        }
        requestCounters();
        updateThread.start();
    }

    /**
     * Reset the counters
     */
    public void reset() {
        for (String aggCounterName : RESET_COUNTERS) {
            AggregateCounter aggCounter = (AggregateCounter) counterMap.get(aggCounterName);
            aggCounter.reset();
        }
        for (String slave : slaveMachines) {
            for (String aggCounterName : RESET_COUNTERS) {
                AggregateCounter aggCounter = (AggregateCounter) counterMap.get(slave + "$" + aggCounterName);
                aggCounter.reset();
            }
        }
    }

    public void resetAll() {
        reset();
        for (String aggCounterName : AGG_COUNTERS) {
            AggregateCounter aggCounter = (AggregateCounter) counterMap.get(aggCounterName);
            aggCounter.reset();
        }
        for (String slave : slaveMachines) {
            for (String aggCounterName : AGG_COUNTERS) {
                AggregateCounter aggCounter = (AggregateCounter) counterMap.get(slave + "$" + aggCounterName);
                aggCounter.reset();
            }
        }
    }

    @Override
    public synchronized ICounter getCounter(String name, boolean create) {
        Counter counter = counterMap.get(name);
        if (counter == null) {
            throw new IllegalStateException("request an unknown counter: " + name + "!");
        }
        return counter;
    }

    @Override
    public ICounter getCounter(String machineName, String counterName, boolean create) {
        Counter counter = counterMap.get(machineName + "$" + counterName);
        if (counter == null) {
            throw new IllegalStateException(
                    "request an unknown counter: " + counterName + " on slave machine " + machineName + "!");
        }
        return counter;
    }

    /**
     * request to each slave machine for all the counters
     */
    private synchronized void requestCounters() {
        try {
            reset();
            for (String slave : slaveMachines) {
                String slaveProfile = requestProfile(slave);
                ObjectMapper parser = new ObjectMapper();
                JsonNode jo = parser.readTree(slaveProfile);
                JsonNode counterObject = jo.get("result");
                if (counterObject.isObject()) {
                    updateCounterMapWithArrayNode(slave, counterObject);
                }
            }
        } catch (Exception e) {
            //ignore
        }
    }

    /**
     * Update counters
     *
     * @param jo
     *            the Profile JSON object
     */
    private void updateCounterMapWithArrayNode(String slave, JsonNode jo) {
        for (String counterName : RESET_COUNTERS) {
            updateCounter(slave, jo, counterName);
        }

        for (String counterName : AGG_COUNTERS) {
            updateCounter(slave, jo, counterName);
        }
    }

    private void updateCounter(String slave, JsonNode jo, String counterName) {
        JsonNode counterObject = jo.get(counterName);
        long counterValue = extractCounterValue(counterObject);
        // global counter
        ICounter counter = getCounter(counterName, true);
        counter.set(counterValue);
        //local counters
        ICounter localCounter = getCounter(slave, counterName, true);
        localCounter.set(counterValue);
    }

    private long extractCounterValue(JsonNode counterObject) {
        long counterValue = 0;
        if (counterObject == null) {
            return counterValue;
        } else if (counterObject.isObject()) {
            /**
             * use the last non-zero value as the counter value
             */
            for (Iterator<JsonNode> jsonIt = counterObject.iterator(); jsonIt.hasNext();) {
                JsonNode value = jsonIt.next();
                if (value.isDouble()) {
                    double currentVal = value.asDouble();
                    if (currentVal != 0) {
                        counterValue = (long) currentVal;
                    }
                } else if (value.isLong()) {
                    long currentVal = value.asLong();
                    if (currentVal != 0) {
                        counterValue = currentVal;
                    }
                }
            }
        } else {
            counterValue = counterObject.asLong();
        }
        return counterValue;
    }

    /**
     * Request a counter from the slave machine
     *
     * @param slaveMachine
     * @return the JSON string from the slave machine
     */
    private String requestProfile(String slaveMachine) {
        try {
            String url = baseURL + slaveMachine;
            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
            con.setRequestMethod("GET");
            int responseCode = con.getResponseCode();
            if (responseCode != 200) {
                throw new IllegalStateException("The http response code is wrong: " + responseCode);
            }
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            return response.toString();
        } catch (Exception e) {
            if (!(e instanceof java.net.ConnectException || e instanceof IOException)) {
                throw new IllegalStateException(e);
            } else {
                return "";
            }
        }
    }

    /**
     * Stop the counter profiler
     */
    public void stop() {
        synchronized (updateThread) {
            stopped = true;
            updateThread.notifyAll();
        }
        try {
            updateThread.join();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * The thread keep updating counters
     */
    private class UpdateThread extends Thread {

        @Override
        public synchronized void run() {
            try {
                while (true) {
                    if (stopped) {
                        break;
                    }
                    requestCounters();
                    wait(UPDATE_INTERVAL);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                notifyAll();
            }
        }

    }

}
