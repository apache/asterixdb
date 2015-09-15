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
package org.apache.asterix.aoya;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import org.apache.asterix.common.configuration.AsterixConfiguration;
import org.apache.asterix.event.schema.yarnCluster.Cluster;
import org.apache.asterix.event.schema.yarnCluster.Node;

public class Utils {

    private Utils() {

    }

    private static final String CONF_DIR_REL = AsterixYARNClient.CONF_DIR_REL;

    public static String hostFromContainerID(String containerID) {
        return containerID.split("_")[4];
    }

    /**
     * Gets the metadata node from an AsterixDB cluster description file
     * 
     * @param cluster
     *            The cluster description in question.
     * @return
     */
    public static Node getMetadataNode(Cluster cluster) {
        Node metadataNode = null;
        if (cluster.getMetadataNode() != null) {
            for (Node node : cluster.getNode()) {
                if (node.getId().equals(cluster.getMetadataNode())) {
                    metadataNode = node;
                    break;
                }
            }
        } else {
            //I will pick one for you.
            metadataNode = cluster.getNode().get(1);
        }
        return metadataNode;
    }

    /**
     * Sends a "poison pill" message to an AsterixDB instance for it to shut down safely.
     * 
     * @param host
     *            The host to shut down.
     * @throws IOException
     */

    public static void sendShutdownCall(String host, int port) throws IOException {
        final String url = "http://" + host + ":" + port + "/admin/shutdown";
        PostMethod method = new PostMethod(url);
        try {
            executeHTTPCall(method);
        } catch (NoHttpResponseException e) {
            //do nothing... this is expected
        }
        //now let's test that the instance is really down, or throw an exception
        try {
            executeHTTPCall(method);
        } catch (ConnectException e) {
            return;
        }
        throw new IOException("Instance did not shut down cleanly.");
    }

    /**
     * Simple test via the AsterixDB Javascript API to determine if an instance is truly live or not.
     * Queries the Metadata dataset and returns true if the query completes successfully, false otherwise.
     * 
     * @param host
     *            The host to run the query against
     * @return
     *         True if the instance is OK, false otherwise.
     * @throws IOException
     */
    public static boolean probeLiveness(String host, int port) throws IOException {
        final String url = "http://" + host + ":" + port + "/query";
        final String test = "for $x in dataset Metadata.Dataset return $x;";
        GetMethod method = new GetMethod(url);
        method.setQueryString(new NameValuePair[] { new NameValuePair("query", test) });
        InputStream response;
        try {
            response = executeHTTPCall(method);
        } catch (ConnectException e) {
            return false;
        }
        if (response == null) {
            return false;
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(response));
        String result = br.readLine();
        if (result == null) {
            return false;
        }
        if(method.getStatusCode() != HttpStatus.SC_OK){
            return false;
        }
        return true;
    }

    private static InputStream executeHTTPCall(HttpMethod method) throws HttpException, IOException {
        HttpClient client = new HttpClient();
        HttpMethodRetryHandler noop = new HttpMethodRetryHandler() {
            @Override
            public boolean retryMethod(final HttpMethod method, final IOException exception, int executionCount) {
                return false;
            }
        };
        client.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, noop);
        client.executeMethod(method);
        return method.getResponseBodyAsStream();
    }

    //**

    public static String makeDots(int iter) {
        int pos = iter % 3;
        char[] dots = { ' ', ' ', ' ' };
        dots[pos] = '.';
        return new String(dots);
    }

    public static boolean confirmAction(String warning) {
        System.out.println(warning);
        System.out.print("Are you sure you want to do this? (yes/no): ");
        Scanner in = new Scanner(System.in);
        while (true) {
            try {
                String input = in.nextLine();
                if ("yes".equals(input)) {
                    return true;
                } else if ("no".equals(input)) {
                    return false;
                } else {
                    System.out.println("Please type yes or no");
                }
            } finally {
                in.close();
            }
        }
    }

    /**
     * Lists the deployed instances of AsterixDB on a YARN cluster
     * 
     * @param conf
     *            Hadoop configuration object
     * @param confDirRel
     *            Relative AsterixDB configuration path for DFS
     * @throws IOException
     */

    public static void listInstances(Configuration conf, String confDirRel) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path instanceFolder = new Path(fs.getHomeDirectory(), confDirRel);
        if (!fs.exists(instanceFolder)) {
            System.out.println("No running or stopped AsterixDB instances exist in this cluster.");
            return;
        }
        FileStatus[] instances = fs.listStatus(instanceFolder);
        if (instances.length != 0) {
            System.out.println("Existing AsterixDB instances: ");
            for (int i = 0; i < instances.length; i++) {
                FileStatus st = instances[i];
                String name = st.getPath().getName();
                ApplicationId lockFile = AsterixYARNClient.getLockFile(name, conf);
                if (lockFile != null) {
                    System.out.println("Instance " + name + " is running with Application ID: " + lockFile.toString());
                } else {
                    System.out.println("Instance " + name + " is stopped");
                }
            }
        } else {
            System.out.println("No running or stopped AsterixDB instances exist in this cluster");
        }
    }

    /**
     * Lists the backups in the DFS.
     * 
     * @param conf
     *            YARN configuration
     * @param confDirRel
     *            Relative config path
     * @param instance
     *            Instance name
     * @throws IOException
     */
    public static void listBackups(Configuration conf, String confDirRel, String instance) throws IOException {
        List<String> backups = getBackups(conf,confDirRel,instance);
        if (backups.size() != 0) {
            System.out.println("Backups for instance " + instance + ": ");
            for (String name : backups) {
                System.out.println("Backup: " + name);
            }
        } else {
            System.out.println("No backups found for instance " + instance + ".");
        }
    }
   /**
    * Return the available snapshot names 
    * @param conf 
    * @param confDirRel
    * @param instance
    * @return
    * @throws IOException
    */
    public static List<String> getBackups(Configuration conf, String confDirRel, String instance) throws IOException{
        FileSystem fs = FileSystem.get(conf);
        Path backupFolder = new Path(fs.getHomeDirectory(), confDirRel + "/" + instance + "/" + "backups");
        FileStatus[] backups = fs.listStatus(backupFolder);
        List<String> backupNames = new ArrayList<String>();
        for(FileStatus f: backups){
            backupNames.add(f.getPath().getName());
        }
        return backupNames;
    }

    /**
     * Removes backup snapshots from the DFS
     * 
     * @param conf
     *            DFS Configuration
     * @param confDirRel
     *            Configuration relative directory
     * @param instance
     *            The asterix instance name
     * @param timestamp
     *            The snapshot timestap (ID)
     * @throws IOException
     */
    public static void rmBackup(Configuration conf, String confDirRel, String instance, long timestamp)
            throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path backupFolder = new Path(fs.getHomeDirectory(), confDirRel + "/" + instance + "/" + "backups");
        FileStatus[] backups = fs.listStatus(backupFolder);
        if (backups.length != 0) {
            System.out.println("Backups for instance " + instance + ": ");
        } else {
            System.out.println("No backups found for instance " + instance + ".");
        }
        for (FileStatus f : backups) {
            String name = f.getPath().getName();
            long file_ts = Long.parseLong(name);
            if (file_ts == timestamp) {
                System.out.println("Deleting backup " + timestamp);
                if (!fs.delete(f.getPath(), true)) {
                    System.out.println("Backup could not be deleted");
                    return;
                } else {
                    return;
                }
            }
        }
        System.out.println("No backup found with specified timestamp");

    }

    /**
     * Simply parses out the YARN cluster config and instantiates it into a nice object.
     * 
     * @return The object representing the configuration
     * @throws FileNotFoundException
     * @throws JAXBException
     */
    public static Cluster parseYarnClusterConfig(String path) throws YarnException {
        try {
            File f = new File(path);
            JAXBContext configCtx = JAXBContext.newInstance(Cluster.class);
            Unmarshaller unmarshaller = configCtx.createUnmarshaller();
            Cluster cl = (Cluster) unmarshaller.unmarshal(f);
            return cl;
        } catch (JAXBException e) {
            throw new YarnException(e);
        }
    }

    public static void writeYarnClusterConfig(String path, Cluster cl) throws YarnException {
        try {
            File f = new File(path);
            JAXBContext configCtx = JAXBContext.newInstance(Cluster.class);
            Marshaller marhsaller = configCtx.createMarshaller();
            marhsaller.marshal(cl, f);
        } catch (JAXBException e) {
            throw new YarnException(e);
        }
    }

    /**
     * Looks in the current class path for AsterixDB libraries and gets the version number from the name of the first match.
     * 
     * @return The version found, as a string.
     */

    public static String getAsterixVersionFromClasspath() {
        String[] cp = System.getProperty("java.class.path").split(System.getProperty("path.separator"));
        String asterixJarPattern = "^(asterix).*(jar)$"; //starts with asterix,ends with jar

        for (String j : cp) {
            //escape backslashes for windows
            String[] pathComponents = j.split(Pattern.quote(File.separator));
            if (pathComponents[pathComponents.length - 1].matches(asterixJarPattern)) {
                //get components of maven version
                String[] byDash = pathComponents[pathComponents.length - 1].split("-");
                //get the version number but remove the possible '.jar' tailing it
                String version = (byDash[2].split("\\."))[0];
                //SNAPSHOT suffix
                if (byDash.length == 4) {
                    //do the same if it's a snapshot suffix
                    return version + '-' + (byDash[3].split("\\."))[0];
                }
                //stable version
                return version;
            }
        }
        return null;
    }

    public static boolean waitForLiveness(ApplicationId appId, boolean probe, boolean print, String message,
            YarnClient yarnClient, String instanceName, Configuration conf, int port) throws YarnException {
        ApplicationReport report;
        try {
            report = yarnClient.getApplicationReport(appId);
        } catch (IOException e) {
            throw new YarnException(e);
        }
        YarnApplicationState st = report.getYarnApplicationState();
        for (int i = 0; i < 120; i++) {
            if (st != YarnApplicationState.RUNNING) {
                try {
                    report = yarnClient.getApplicationReport(appId);
                    st = report.getYarnApplicationState();
                    if (print) {
                        System.out.print(message + Utils.makeDots(i) + "\r");
                    }
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                } catch (IOException e1) {
                    throw new YarnException(e1);
                }
                if (st == YarnApplicationState.FAILED || st == YarnApplicationState.FINISHED
                        || st == YarnApplicationState.KILLED) {
                    return false;
                }
            }
            if (probe) {
                String host;
                host = getCCHostname(instanceName, conf);
                try {
                    for (int j = 0; j < 60; j++) {
                        if (!Utils.probeLiveness(host, port)) {
                            try {
                                if (print) {
                                    System.out.print(message + Utils.makeDots(i) + "\r");
                                }
                                Thread.sleep(1000);
                            } catch (InterruptedException e2) {
                                Thread.currentThread().interrupt();
                            }
                        } else {
                            if (print) {
                                System.out.println("");
                            }
                            return true;
                        }
                    }
                } catch (IOException e1) {
                    throw new YarnException(e1);
                }
            } else {
                if (print) {
                    System.out.println("");
                }
                return true;
            }
        }
        if (print) {
            System.out.println("");
        }
        return false;
    }

    public static boolean waitForLiveness(ApplicationId appId, String message, YarnClient yarnClient,
            String instanceName, Configuration conf, int port) throws YarnException, IOException {
        return waitForLiveness(appId, true, true, message, yarnClient, instanceName, conf, port);
    }

    public static boolean waitForApplication(ApplicationId appId, YarnClient yarnClient, String message, int port)
            throws YarnException, IOException {
        return waitForLiveness(appId, false, true, message, yarnClient, "", null, port);
    }

    public static boolean waitForApplication(ApplicationId appId, YarnClient yarnClient, int port) throws YarnException,
            IOException, JAXBException {
        return waitForLiveness(appId, false, false, "", yarnClient, "", null, port);
    }

    public static String getCCHostname(String instanceName, Configuration conf) throws YarnException {
        try {
            FileSystem fs = FileSystem.get(conf);
            String instanceFolder = instanceName + "/";
            String pathSuffix = CONF_DIR_REL + instanceFolder + "cluster-config.xml";
            Path dstConf = new Path(fs.getHomeDirectory(), pathSuffix);
            File tmp = File.createTempFile("cluster-config", "xml");
            tmp.deleteOnExit();
            fs.copyToLocalFile(dstConf, new Path(tmp.getPath()));
            JAXBContext clusterConf = JAXBContext.newInstance(Cluster.class);
            Unmarshaller unm = clusterConf.createUnmarshaller();
            Cluster cl = (Cluster) unm.unmarshal(tmp);
            String ccIp = cl.getMasterNode().getClientIp();
            return ccIp;
        } catch (IOException | JAXBException e) {
            throw new YarnException(e);
        }
    }

    public static AsterixConfiguration loadAsterixConfig(String path) throws IOException {
        File f = new File(path);
        try {
            JAXBContext configCtx = JAXBContext.newInstance(AsterixConfiguration.class);
            Unmarshaller unmarshaller = configCtx.createUnmarshaller();
            AsterixConfiguration conf = (AsterixConfiguration) unmarshaller.unmarshal(f);
            return conf;
        } catch (JAXBException e) {
            throw new IOException(e);
        }
    }

}
