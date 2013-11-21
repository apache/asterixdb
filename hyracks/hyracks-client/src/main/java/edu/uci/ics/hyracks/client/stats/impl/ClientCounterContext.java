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
package edu.uci.ics.hyracks.client.stats.impl;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import edu.uci.ics.hyracks.api.job.profiling.counters.ICounter;
import edu.uci.ics.hyracks.api.job.profiling.counters.ICounterContext;
import edu.uci.ics.hyracks.client.stats.AggregateCounter;
import edu.uci.ics.hyracks.client.stats.Counters;
import edu.uci.ics.hyracks.control.common.job.profiling.counters.Counter;

/**
 * @author yingyib
 */
public class ClientCounterContext implements ICounterContext {
    private static String[] RESET_COUNTERS = { Counters.NETWORK_IO_READ, Counters.NETWORK_IO_WRITE,
            Counters.MEMORY_USAGE, Counters.DISK_READ, Counters.DISK_WRITE };
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
    }

    public void resetAll() {
        reset();
        for (String aggCounterName : AGG_COUNTERS) {
            AggregateCounter aggCounter = (AggregateCounter) counterMap.get(aggCounterName);
            aggCounter.reset();
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

    /**
     * request to each slave machine for all the counters
     */
    private synchronized void requestCounters() {
        try {
            reset();
            for (String slave : slaveMachines) {
                String slaveProfile = requestProfile(slave);
                JSONParser parser = new JSONParser();
                JSONObject jo = (JSONObject) parser.parse(slaveProfile);
                updateCounterMap((JSONObject) jo.get("result"));
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Update counters
     * 
     * @param jo
     *            the Profile JSON object
     */
    private void updateCounterMap(JSONObject jo) {
        for (String counterName : RESET_COUNTERS) {
            JSONArray jArray = (JSONArray) jo.get(counterName);
            if(jArray==null){
                continue; 
            }
            Object[] values = jArray.toArray();
            long counterValue = 0;
            for (Object value : values) {
                if (value instanceof Double) {
                    Double dValue = (Double) value;
                    counterValue += dValue.doubleValue();
                } else if (value instanceof Long) {
                    Long lValue = (Long) value;
                    counterValue += lValue.longValue();
                }
            }
            counterValue /= values.length;
            ICounter counter = getCounter(counterName, true);
            counter.set(counterValue);
        }
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
            throw new IllegalStateException(e);
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
