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
package edu.uci.ics.hyracks.hadoop.compat.util;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

import edu.uci.ics.dcache.client.DCacheClient;
import edu.uci.ics.dcache.client.DCacheClientConfig;

public class DCacheHandler {

    String clientPropertyFile;
    String key;
    String valueFilePath;

    public enum Operation {
        GET,
        PUT,
        DELETE
    }

    private static DCacheClient dCacheClient;
    private static final String[] operations = { "GET", "PUT", "DELETE" };

    public DCacheHandler(String clientPropertyFile) throws Exception {
        this.clientPropertyFile = clientPropertyFile;
        init();
    }

    private void init() throws Exception {
        dCacheClient = DCacheClient.get();
        DCacheClientConfig dcacheClientConfig = new DCacheClientConfig();
        dCacheClient.init(dcacheClientConfig);
    }

    public static DCacheClient getInstance(String clientPropertyFile) {
        if (dCacheClient == null) {
            dCacheClient = DCacheClient.get();
        }
        return dCacheClient;
    }

    public String getClientPropertyFile() {
        return clientPropertyFile;
    }

    public void put(String key, String value) throws IOException {
        dCacheClient.set(key, value);
        System.out.println(" added to cache " + key + " : " + value);
    }

    public Object get(String key) throws IOException {
        return dCacheClient.get(key);
    }

    public void delete(String key) throws IOException {
        dCacheClient.delete(key);
    }

    public Object performOperation(String operation, String[] args) throws Exception {
        Object returnValue = null;
        int operationIndex = getOperation(operation);
        switch (operationIndex) {
            case 0:
                returnValue = dCacheClient.get(args[2]);
                System.out.println(" get from cache " + returnValue);
                break;
            case 1:
                dCacheClient.set(args[2], args[3]);
                System.out.println(" added to cache " + args[2] + " : " + args[3]);
                break;
            case 2:
                dCacheClient.delete(args[2]);
                System.out.println(" removed from cache " + args[2]);
                break;
            default:
                System.out.println("Error : Operation not supported !");
                break;
        }
        return returnValue;
    }

    private int getOperation(String operation) {
        for (int i = 0; i < operations.length; i++) {
            if (operations[i].equalsIgnoreCase(operation)) {
                return i;
            }
        }
        return -1;
    }

}
