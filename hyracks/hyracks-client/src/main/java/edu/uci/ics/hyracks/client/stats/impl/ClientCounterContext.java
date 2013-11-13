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
import edu.uci.ics.hyracks.client.stats.Counters;
import edu.uci.ics.hyracks.control.common.job.profiling.counters.Counter;

/**
 * @author yingyib
 */
public class ClientCounterContext implements ICounterContext {
    private String[] counters = { Counters.SYSTEM_LOAD, Counters.NETWORK_IO_READ, Counters.NETWORK_IO_WRITE,
            Counters.MEMORY_USAGE };
    private Map<String, Counter> counterMap = new HashMap<String, Counter>();
    private final String baseURL;
    private final List<String> slaveMachines = new ArrayList<String>();

    public ClientCounterContext(String hostName, int restPort, Collection<String> slaveMachines) {
        this.baseURL = "http://" + hostName + ":" + restPort + "/rest/nodes/";
        this.slaveMachines.addAll(slaveMachines);
        requestCounters();
    }

    @Override
    public ICounter getCounter(String name, boolean create) {
        Counter counter = counterMap.get(name);
        if (counter == null && create) {
            counter = new Counter(name);
            counterMap.put(name, counter);
        }
        return counter;
    }

    private void requestCounters() {
        try {
            for (String slave : slaveMachines) {
                String slaveProfile = sendGet(slave);
                JSONParser parser = new JSONParser();
                JSONObject jo = (JSONObject) parser.parse(slaveProfile);
                updateCounterMap((JSONObject) jo.get("result"));
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private void updateCounterMap(JSONObject jo) {
        for (String counterName : counters) {
            JSONArray jArray = (JSONArray) jo.get(counterName);
            Object[] values = jArray.toArray();
            long counterValue = 0;
            for (Object value : values) {
                if (value instanceof Double) {
                    Double dValue = (Double) value;
                    counterValue += dValue.doubleValue() * 100;
                } else if (value instanceof Long) {
                    Long lValue = (Long) value;
                    counterValue += lValue.longValue();
                }
            }
            counterValue /= slaveMachines.size();
            ICounter counter = getCounter(counterName, true);
            counter.set(counterValue);
        }
    }

    private String sendGet(String slaveMachine) {
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

}
