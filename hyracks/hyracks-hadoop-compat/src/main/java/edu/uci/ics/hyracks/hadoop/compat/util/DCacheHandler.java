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
