/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.aoya;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.event.schema.yarnCluster.Cluster;
import edu.uci.ics.asterix.event.schema.yarnCluster.MasterNode;
import edu.uci.ics.asterix.event.schema.yarnCluster.Node;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class AsterixApplicationMaster {

    static
    {
        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.INFO);
        rootLogger.addAppender(new ConsoleAppender(
                new PatternLayout("%-6r [%p] %c - %m%n")));
    }
    private static final Log LOG = LogFactory.getLog(AsterixApplicationMaster.class);
    private static final String CLUSTER_DESC_PATH = "cluster-config.xml";
    private static final String ASTERIX_CONF_NAME = "asterix-configuration.xml";
    private static final String ASTERIX_ZIP_NAME = "asterix-server.zip";

    private static final int CC_MEMORY_MBS_DEFAULT = 1024;
    private static final int NC_MEMORY_MBS_DEFAULT = 1536;
    private static final String EXTERNAL_CC_JAVA_OPTS_DEFAULT = "-Xmx" + CC_MEMORY_MBS_DEFAULT + "m";
    private static final String EXTERNAL_NC_JAVA_OPTS_DEFAULT = "-Xmx" + NC_MEMORY_MBS_DEFAULT + "m";
    private static final String OBLITERATOR_CLASSNAME = "edu.uci.ics.asterix.aoya.Deleter";
    private static final String HDFS_BACKUP_CLASSNAME = "edu.uci.ics.asterix.aoya.HDFSBackup";
    private static final String NC_CLASSNAME = "edu.uci.ics.hyracks.control.nc.NCDriver";
    private static final String CC_CLASSNAME = "edu.uci.ics.hyracks.control.cc.CCDriver";
    private static final String JAVA_HOME = System.getProperty("java.home");
    private boolean doneAllocating = false;

    // Configuration
    private Configuration conf;

    // Handle to communicate with the Resource Manager
    private AMRMClientAsync<ContainerRequest> resourceManager;

    // Handle to communicate with the Node Manager
    private NMClientAsync nmClientAsync;
    // Listen to process the response from the Node Manager
    private NMCallbackHandler containerListener;
    // Application Attempt Id ( combination of attemptId and fail count )
    private ApplicationAttemptId appAttemptID;

    // TODO
    // For status update for clients - yet to be implemented
    // Hostname of the container
    private String appMasterHostname = "";
    // Port on which the app master listens for status updates from clients
    private int appMasterRpcPort = new Random().nextInt(65535-49152);
    // Tracking url to which app master publishes info for clients to monitor
    private String appMasterTrackingUrl = "";

    // Counter for completed containers ( complete denotes successful or failed )
    private AtomicInteger numCompletedContainers = new AtomicInteger();
    // Allocated container count so that we know how many containers has the RM
    // allocated to us
    private AtomicInteger numAllocatedContainers = new AtomicInteger();
    // Count of failed containers
    private AtomicInteger numFailedContainers = new AtomicInteger();
    // Count of containers already requested from the RM
    // Needed as once requested, we should not request for containers again.
    // Only request for more if the original requirement changes.
    private AtomicInteger numRequestedContainers = new AtomicInteger();
    //Tells us whether the Cluster Controller is up so we can safely start some Node Controllers
    private AtomicBoolean ccUp = new AtomicBoolean();
    private AtomicBoolean ccStarted = new AtomicBoolean();
    private Queue<Node> pendingNCs = new ArrayDeque<Node>();

    //HDFS path to AsterixDB distributable zip
    private String asterixZipPath = "";
    // Timestamp needed for creating a local resource
    private long asterixZipTimestamp = 0;
    // File length needed for local resource
    private long asterixZipLen = 0;

    //HDFS path to AsterixDB cluster description
    private String asterixConfPath = "";
    // Timestamp needed for creating a local resource
    private long asterixConfTimestamp = 0;
    // File length needed for local resource
    private long asterixConfLen = 0;

    private String instanceConfPath = "";

    //base dir under which all configs and binaries lie
    private String dfsBasePath;

    private int numTotalContainers = 0;

    // Set the local resources
    private Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

    private Cluster clusterDesc = null;
    private MasterNode cC = null;
    private String ccJavaOpts = null;
    private int ccMem = 0;
    private String ncJavaOpts = null;
    private int ncMem = 0;
    private volatile boolean done;
    private volatile boolean success;

    private boolean obliterate = false;
    private Path appMasterJar = null;
    private boolean backup = false;
    long backupTimestamp;
    String snapName;
    private boolean restore = false;
    private boolean initial = false;

    // Launch threads
    private List<Thread> launchThreads = new CopyOnWriteArrayList<Thread>();

    public static void main(String[] args) {

        boolean result = false;
        try {

            AsterixApplicationMaster appMaster = new AsterixApplicationMaster();
            LOG.info("Initializing ApplicationMaster");
            appMaster.setEnvs(appMaster.setArgs(args));
            boolean doRun = appMaster.init();
            if (!doRun) {
                System.exit(0);
            }
            result = appMaster.run();
        } catch (Exception e) {
            LOG.fatal("Error running ApplicationMaster", e);
            System.exit(1);
        }
        if (result) {
            LOG.info("Application Master completed successfully. exiting");
            System.exit(0);
        } else {
            LOG.info("Application Master failed. exiting");
            System.exit(2);
        }
    }

    private void dumpOutDebugInfo() {

        LOG.info("Dump debug output");
        Map<String, String> envs = System.getenv();
        for (Map.Entry<String, String> env : envs.entrySet()) {
            LOG.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
            System.out.println("System env: key=" + env.getKey() + ", val=" + env.getValue());
        }

        String cmd = "ls -alhLR";
        Runtime run = Runtime.getRuntime();
        Process pr = null;
        try {
            pr = run.exec(cmd);
            pr.waitFor();

            BufferedReader buf = new BufferedReader(new InputStreamReader(pr.getInputStream()));
            String line = "";
            while ((line = buf.readLine()) != null) {
                LOG.info("System CWD content: " + line);
                System.out.println("System CWD content: " + line);
            }
            buf.close();
        } catch (IOException e) {
            LOG.info(e);
        } catch (InterruptedException e) {
            LOG.info(e);
        }
    }

    public AsterixApplicationMaster() {
        // Set up the configuration and RPC
        conf = new YarnConfiguration();

    }

    public CommandLine setArgs(String[] args) throws ParseException {
        Options opts = new Options();
        opts.addOption("app_attempt_id", true, "App Attempt ID. Not to be used unless for testing purposes");
        opts.addOption("priority", true, "Application Priority. Default 0");
        opts.addOption("debug", false, "Dump out debug information");
        opts.addOption("help", false, "Print usage");
        opts.addOption("initial", false, "Initialize existing Asterix instance.");
        opts.addOption("obliterate", false, "Delete asterix instance completely.");
        opts.addOption("backup", false, "Back up AsterixDB instance");
        opts.addOption("restore", true, "Restore an AsterixDB instance");

        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (cliParser.hasOption("help")) {
            printUsage(opts);
        }

        if (cliParser.hasOption("debug")) {
            dumpOutDebugInfo();
        }

        if (cliParser.hasOption("obliterate")) {
            obliterate = true;
        }
        if(cliParser.hasOption("initial")){
            initial = true;
        }

        if (cliParser.hasOption("backup")) {
            backup = true;
            backupTimestamp = System.currentTimeMillis();
        }
        if (cliParser.hasOption("restore")) {
            restore = true;
            snapName = cliParser.getOptionValue("restore");
            LOG.info(snapName);
        }
        return cliParser;
    }

    public void setEnvs(CommandLine cliParser) {
        Map<String, String> envs = System.getenv();
        if (envs.containsKey("HADOOP_CONF_DIR")) {
            File hadoopConfDir = new File(envs.get("HADOOP_CONF_DIR"));
            if (hadoopConfDir.isDirectory()) {
                for (File config : hadoopConfDir.listFiles()) {
                    if (config.getName().matches("^.*(xml)$")) {
                        conf.addResource(new Path(config.getAbsolutePath()));
                    }
                }
            }
        }
        //the containerID might be in the arguments or the environment
        if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
            if (cliParser.hasOption("app_attempt_id")) {
                String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
                appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
            } else {

                throw new IllegalArgumentException(
                        "Environment is not set correctly- please check client submission settings");
            }
        } else {
            ContainerId containerId = ConverterUtils.toContainerId(envs.get(Environment.CONTAINER_ID.name()));
            appAttemptID = containerId.getApplicationAttemptId();
        }

        if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)
                || !envs.containsKey(Environment.NM_HOST.name()) || !envs.containsKey(Environment.NM_HTTP_PORT.name())
                || !envs.containsKey(Environment.NM_PORT.name())) {
            throw new IllegalArgumentException(
                    "Environment is not set correctly- please check client submission settings");
        }
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, envs.get("PWD") + File.separator + "bin" + File.separator
                + ASTERIX_CONF_NAME);

        LOG.info("Application master for app" + ", appId=" + appAttemptID.getApplicationId().getId()
                + ", clustertimestamp=" + appAttemptID.getApplicationId().getClusterTimestamp() + ", attemptId="
                + appAttemptID.getAttemptId());

        asterixZipPath = envs.get(AConstants.TARLOCATION);
        asterixZipTimestamp = Long.parseLong(envs.get(AConstants.TARTIMESTAMP));
        asterixZipLen = Long.parseLong(envs.get(AConstants.TARLEN));

        asterixConfPath = envs.get(AConstants.CONFLOCATION);
        asterixConfTimestamp = Long.parseLong(envs.get(AConstants.CONFTIMESTAMP));
        asterixConfLen = Long.parseLong(envs.get(AConstants.CONFLEN));

        instanceConfPath = envs.get(AConstants.INSTANCESTORE);
        //the only time this is null is during testing, when asterix-yarn isn't packaged in a JAR yet.
        if(envs.get(AConstants.APPLICATIONMASTERJARLOCATION) != null
                && !envs.get(AConstants.APPLICATIONMASTERJARLOCATION).endsWith(File.separator)){
            appMasterJar = new Path(envs.get(AConstants.APPLICATIONMASTERJARLOCATION));
        }
        else{
            appMasterJar = null;
        }

        dfsBasePath = envs.get(AConstants.DFS_BASE);
        //If the NM has an odd environment where the proper hadoop XML configs dont get imported, we can end up not being able to talk to the RM
        // this solves that!
        //in a testing environment these can be null however. 
        if (envs.get(AConstants.RMADDRESS) != null) {
            conf.set("yarn.resourcemanager.address", envs.get(AConstants.RMADDRESS));
            LOG.info("RM Address: " + envs.get(AConstants.RMADDRESS));
        }
        if (envs.get(AConstants.RMADDRESS) != null) {
            conf.set("yarn.resourcemanager.scheduler.address", envs.get(AConstants.RMSCHEDULERADDRESS));
        }
        ccJavaOpts = envs.get(AConstants.CC_JAVA_OPTS);
        //set defaults if no special given options
        if (ccJavaOpts == null) {
            ccJavaOpts = EXTERNAL_CC_JAVA_OPTS_DEFAULT;
        }
        ncJavaOpts = envs.get(AConstants.NC_JAVA_OPTS);
        if (ncJavaOpts == null) {
            ncJavaOpts = EXTERNAL_NC_JAVA_OPTS_DEFAULT;
        }

        LOG.info("Path suffix: " + instanceConfPath);
    }

    public boolean init() throws ParseException, IOException, AsterixException, YarnException {
        try {
            localizeDFSResources();
            clusterDesc = Utils.parseYarnClusterConfig(CLUSTER_DESC_PATH);
            cC = clusterDesc.getMasterNode();
            appMasterTrackingUrl = "http://" + cC.getClientIp() + ":" + cC.getClientPort() + Path.SEPARATOR;
            distributeAsterixConfig();
            //now let's read what's in there so we can set the JVM opts right
            LOG.debug("config file loc: " + System.getProperty(GlobalConfig.CONFIG_FILE_PROPERTY));
        } catch (FileNotFoundException | IllegalStateException e) {
            LOG.error("Could not deserialize Cluster Config from disk- aborting!");
            LOG.error(e);
            throw e;
        }

        return true;
    }

    /**
     * Sets up the parameters for the Asterix config.
     * 
     * @throws IOException
     */
    private void distributeAsterixConfig() throws IOException {
        FileSystem fs = FileSystem.get(conf);
        String pathSuffix = instanceConfPath + File.separator + ASTERIX_CONF_NAME;
        Path dst = new Path(dfsBasePath, pathSuffix);
        URI paramLocation = dst.toUri();
        FileStatus paramFileStatus = fs.getFileStatus(dst);
        Long paramLen = paramFileStatus.getLen();
        Long paramTimestamp = paramFileStatus.getModificationTime();
        LocalResource asterixParamLoc = Records.newRecord(LocalResource.class);
        asterixParamLoc.setType(LocalResourceType.FILE);
        asterixParamLoc.setVisibility(LocalResourceVisibility.PRIVATE);
        asterixParamLoc.setResource(ConverterUtils.getYarnUrlFromURI(paramLocation));
        asterixParamLoc.setTimestamp(paramTimestamp);
        asterixParamLoc.setSize(paramLen);
        localResources.put(ASTERIX_CONF_NAME, asterixParamLoc);

    }

    /**
     * @param c
     *            The cluster exception to attempt to alocate with the RM
     * @throws YarnException
     */
    private void requestResources(Cluster c) throws YarnException, UnknownHostException {
        //set memory
        if (c.getCcContainerMem() != null) {
            ccMem = Integer.parseInt(c.getCcContainerMem());
        } else {
            ccMem = CC_MEMORY_MBS_DEFAULT;
        }
        if (c.getNcContainerMem() != null) {
            ncMem = Integer.parseInt(c.getNcContainerMem());
        } else {
            ncMem = CC_MEMORY_MBS_DEFAULT;
        }
        //request CC
        int numNodes = 0;
        ContainerRequest ccAsk = hostToRequest(cC.getClusterIp(), true);
        resourceManager.addContainerRequest(ccAsk);
        LOG.info("Asked for CC: " + Arrays.toString(ccAsk.getNodes().toArray()));
        numNodes++;
        //now we wait to be given the CC before starting the NCs...
        //we will wait a minute. 
        int deathClock = 60;
        while (ccUp.get() == false && deathClock > 0) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                LOG.debug(ex);
            }
            --deathClock;
        }
        if (deathClock == 0 && ccUp.get() == false) {
            throw new YarnException("Couldn't allocate container for CC. Abort!");
        }
        LOG.info("Waiting for CC process to start");
        //TODO: inspect for actual liveness instead of waiting.
        // is there a good way to do this? maybe try opening a socket to it...
        try {
            Thread.sleep(10000);
        } catch (InterruptedException ex) {
            LOG.debug(ex);
        }
        //request NCs
        for (Node n : c.getNode()) {
            resourceManager.addContainerRequest(hostToRequest(n.getClusterIp(), false));
            LOG.info("Asked for NC: " + n.getClusterIp());
            numNodes++;
            synchronized(pendingNCs){
                pendingNCs.add(n);
            }
        }
        LOG.info("Requested all NCs and CCs. Wait for things to settle!");
        numRequestedContainers.set(numNodes);
        numTotalContainers = numNodes;
        doneAllocating = true;

    }

    /**
     * Asks the RM for a particular host, nicely.
     * 
     * @param host
     *            The host to request
     * @param cc
     *            Whether or not the host is the CC
     * @return A container request that is (hopefully) for the host we asked for.
     */
    private ContainerRequest hostToRequest(String host, boolean cc) throws UnknownHostException {
        InetAddress hostIp = InetAddress.getByName(host);
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(0);
        Resource capability = Records.newRecord(Resource.class);
        if (cc) {
            capability.setMemory(ccMem);
        } else {
            capability.setMemory(ncMem);
        }
        //we dont set anything else because we don't care about that and yarn doesn't honor it yet
        String[] hosts = new String[1];
        //TODO this is silly
        hosts[0] = hostIp.getHostName();
        LOG.info("IP addr: " + host + " resolved to " + hostIp.getHostName());
        ContainerRequest request = new ContainerRequest(capability, hosts, null, pri, false);
        LOG.info("Requested host ask: " + request.getNodes());
        return request;
    }

    /**
     * Determines whether or not a container is the one on which the CC should reside
     * 
     * @param c
     *            The container in question
     * @return True if the container should have the CC process on it, false otherwise.
     */
    boolean containerIsCC(Container c) {
        String containerHost = c.getNodeId().getHost();
        try {
            InetAddress containerIp = InetAddress.getByName(containerHost);
            LOG.info(containerIp.getCanonicalHostName());
            InetAddress ccIp = InetAddress.getByName(cC.getClusterIp());
            LOG.info(ccIp.getCanonicalHostName());
            return containerIp.getCanonicalHostName().equals(ccIp.getCanonicalHostName());
        } catch (UnknownHostException e) {
            return false;
        }
    }

    /**
     * Attempts to find the Node in the Cluster Description that matches this container
     * 
     * @param c
     *            The container to resolve
     * @return The node this container corresponds to
     * @throws java.net.UnknownHostException
     *             if the container isn't present in the description
     */
    Node containerToNode(Container c, Cluster cl) throws UnknownHostException {
        String containerHost = c.getNodeId().getHost();
        InetAddress containerIp = InetAddress.getByName(containerHost);
        LOG.info("Resolved Container IP: " + containerIp);
        for (Node node : cl.getNode()) {
            InetAddress nodeIp = InetAddress.getByName(node.getClusterIp());
            LOG.info(nodeIp + "?=" + containerIp);
            if (nodeIp.equals(containerIp))
                return node;
        }
        //if we find nothing, this is bad...
        throw new java.net.UnknownHostException("Could not resolve container" + containerHost + " to node");
    }

    /**
     * Here I am just pointing the Containers to the exisiting HDFS resources given by the Client
     * filesystem of the nodes.
     * 
     * @throws IOException
     */
    private void localizeDFSResources() throws IOException {
        //if performing an 'offline' task, skip a lot of resource distribution
        if (obliterate || backup || restore) {
            if (appMasterJar == null || ("").equals(appMasterJar)) {
                //this can happen in a jUnit testing environment. we don't need to set it there. 
                if (!conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
                    throw new IllegalStateException("AM jar not provided in environment.");
                } else {
                    return;
                }
            }
            FileSystem fs = FileSystem.get(conf);
            FileStatus appMasterJarStatus = fs.getFileStatus(appMasterJar);
            LocalResource obliteratorJar = Records.newRecord(LocalResource.class);
            obliteratorJar.setType(LocalResourceType.FILE);
            obliteratorJar.setVisibility(LocalResourceVisibility.PRIVATE);
            obliteratorJar.setResource(ConverterUtils.getYarnUrlFromPath(appMasterJar));
            obliteratorJar.setTimestamp(appMasterJarStatus.getModificationTime());
            obliteratorJar.setSize(appMasterJarStatus.getLen());
            localResources.put("asterix-yarn.jar", obliteratorJar);
            LOG.info(localResources.values());
            return;
        }
        //otherwise, distribute evertything to start up asterix

        LocalResource asterixZip = Records.newRecord(LocalResource.class);

        //this un-tar's the asterix distribution
        asterixZip.setType(LocalResourceType.ARCHIVE);

        asterixZip.setVisibility(LocalResourceVisibility.PRIVATE);
        try {
            asterixZip.setResource(ConverterUtils.getYarnUrlFromURI(new URI(asterixZipPath)));

        } catch (URISyntaxException e) {
            LOG.error("Error locating Asterix zip" + " in env, path=" + asterixZipPath);
            throw new IOException(e);
        }

        asterixZip.setTimestamp(asterixZipTimestamp);
        asterixZip.setSize(asterixZipLen);
        localResources.put(ASTERIX_ZIP_NAME, asterixZip);

        //now let's do the same for the cluster description XML
        LocalResource asterixConf = Records.newRecord(LocalResource.class);
        asterixConf.setType(LocalResourceType.FILE);

        asterixConf.setVisibility(LocalResourceVisibility.PRIVATE);
        try {
            asterixConf.setResource(ConverterUtils.getYarnUrlFromURI(new URI(asterixConfPath)));
        } catch (URISyntaxException e) {
            LOG.error("Error locating Asterix config" + " in env, path=" + asterixConfPath);
            throw new IOException(e);
        }
        //TODO: I could avoid localizing this everywhere by only calling this block on the metadata node. 
        asterixConf.setTimestamp(asterixConfTimestamp);
        asterixConf.setSize(asterixConfLen);
        localResources.put("cluster-config.xml", asterixConf);
        //now add the libraries if there are any
        try {
            FileSystem fs = FileSystem.get(conf);
            Path p = new Path(dfsBasePath, instanceConfPath + File.separator + "library" + Path.SEPARATOR);
            if (fs.exists(p)) {
                FileStatus[] dataverses = fs.listStatus(p);
                for (FileStatus d : dataverses) {
                    if (!d.isDirectory())
                        throw new IOException("Library configuration directory structure is incorrect");
                    FileStatus[] libraries = fs.listStatus(d.getPath());
                    for (FileStatus l : libraries) {
                        if (l.isDirectory())
                            throw new IOException("Library configuration directory structure is incorrect");
                        LocalResource lr = Records.newRecord(LocalResource.class);
                        lr.setResource(ConverterUtils.getYarnUrlFromURI(l.getPath().toUri()));
                        lr.setSize(l.getLen());
                        lr.setTimestamp(l.getModificationTime());
                        lr.setType(LocalResourceType.ARCHIVE);
                        lr.setVisibility(LocalResourceVisibility.PRIVATE);
                        localResources.put("library" + Path.SEPARATOR + d.getPath().getName() + Path.SEPARATOR
                                + l.getPath().getName().split("\\.")[0], lr);
                        LOG.info("Found library: " + l.getPath().toString());
                        LOG.info(l.getPath().getName());
                    }
                }
            }
        } catch (FileNotFoundException e) {
            LOG.info("No external libraries present");
            //do nothing, it just means there aren't libraries. that is possible and ok
            // it should be handled by the fs.exists(p) check though.
        }
        LOG.info(localResources.values());

    }

    private void printUsage(Options opts) {
        new HelpFormatter().printHelp("ApplicationMaster", opts);
    }

    /**
     * Start the AM and request all necessary resources.
     * 
     * @return True if the run fully succeeded, false otherwise.
     * @throws YarnException
     * @throws IOException
     */
    public boolean run() throws YarnException, IOException {
        LOG.info("Starting ApplicationMaster");

        AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
        resourceManager = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
        resourceManager.init(conf);
        resourceManager.start();

        containerListener = new NMCallbackHandler();
        nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(conf);
        nmClientAsync.start();

        // Register self with ResourceManager
        // This will start heartbeating to the RM
        try {
            appMasterHostname = InetAddress.getLocalHost().toString();
        } catch (java.net.UnknownHostException uhe) {
            appMasterHostname = uhe.toString();
        }
        RegisterApplicationMasterResponse response = resourceManager.registerApplicationMaster(appMasterHostname,
                appMasterRpcPort, appMasterTrackingUrl);

        // Dump out information about cluster capability as seen by the
        // resource manager
        int maxMem = response.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

        try {
            requestResources(clusterDesc);
        } catch (YarnException e) {
            LOG.error("Could not allocate resources properly:" + e.getMessage());
            done = true;
            throw e;
        }
        //now we just sit and listen for messages from the RM

        while (!done) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException ex) {
            }
        }
        finish();
        return success;
    }

    /**
     * Clean up, whether or not we were successful.
     */
    private void finish() {
        // Join all launched threads
        // needed for when we time out
        // and we need to release containers
        for (Thread launchThread : launchThreads) {
            try {
                launchThread.join(10000);
            } catch (InterruptedException e) {
                LOG.info("Exception thrown in thread join: " + e.getMessage());
                //from https://stackoverflow.com/questions/4812570/how-to-store-printstacktrace-into-a-string
                StringWriter errors = new StringWriter();
                e.printStackTrace(new PrintWriter(errors));
                LOG.error(errors.toString());
            }
        }

        // When the application completes, it should stop all running containers
        LOG.info("Application completed. Stopping running containers");
        nmClientAsync.stop();

        // When the application completes, it should send a finish application
        // signal to the RM
        LOG.info("Application completed. Signalling finish to RM");

        FinalApplicationStatus appStatus;
        String appMessage = null;
        success = true;
        if (numFailedContainers.get() == 0 && numCompletedContainers.get() == numTotalContainers) {
            appStatus = FinalApplicationStatus.SUCCEEDED;
        } else {
            appStatus = FinalApplicationStatus.FAILED;
            appMessage = "Diagnostics." + ", total=" + numTotalContainers + ", completed="
                    + numCompletedContainers.get() + ", allocated=" + numAllocatedContainers.get() + ", failed="
                    + numFailedContainers.get();
            success = false;
        }
        try {
            resourceManager.unregisterApplicationMaster(appStatus, appMessage, null);
        } catch (YarnException ex) {
            LOG.error("Failed to unregister application", ex);
        } catch (IOException e) {
            LOG.error("Failed to unregister application", e);
        }
        done = true;
        resourceManager.stop();
    }

    /**
     * This handles the information that comes in from the RM while the AM
     * is running.
     */
    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
        public void onContainersCompleted(List<ContainerStatus> completedContainers) {
            LOG.info("Got response from RM for container ask, completedCnt=" + completedContainers.size());
            for (ContainerStatus containerStatus : completedContainers) {
                LOG.info("Got container status for containerID=" + containerStatus.getContainerId() + ", state="
                        + containerStatus.getState() + ", exitStatus=" + containerStatus.getExitStatus()
                        + ", diagnostics=" + containerStatus.getDiagnostics());

                // non complete containers should not be here
                if(containerStatus.getState() != ContainerState.COMPLETE){
                    throw new IllegalStateException("Non-completed container given as completed by RM.");
                }

                // increment counters for completed/failed containers
                int exitStatus = containerStatus.getExitStatus();
                if (0 != exitStatus) {
                    // container failed
                    numCompletedContainers.incrementAndGet();
                    numFailedContainers.incrementAndGet();
                } else {
                    // nothing to do
                    // container completed successfully
                    numCompletedContainers.incrementAndGet();
                    LOG.info("Container completed successfully." + ", containerId=" + containerStatus.getContainerId());
                }
            }
            //stop infinite looping of run()
            if (numCompletedContainers.get() + numFailedContainers.get() == numAllocatedContainers.get()
                    && doneAllocating)
                done = true;
        }

        public void onContainersAllocated(List<Container> allocatedContainers) {
            LOG.info("Got response from RM for container ask, allocatedCnt=" + allocatedContainers.size());
            numAllocatedContainers.addAndGet(allocatedContainers.size());
            for (Container allocatedContainer : allocatedContainers) {
                synchronized(pendingNCs){
                    try {
                        if (!pendingNCs.contains(containerToNode(allocatedContainer, clusterDesc)) && ccUp.get()) {
                            nmClientAsync.stopContainerAsync(allocatedContainer.getId(), allocatedContainer.getNodeId());
                            continue;
                        }
                    } catch(UnknownHostException ex){
                        LOG.error("Unknown host allocated for us by RM- this shouldn't happen.", ex);
                    }
                }
                LOG.info("Launching shell command on a new container." + ", containerId=" + allocatedContainer.getId()
                        + ", containerNode=" + allocatedContainer.getNodeId().getHost() + ":"
                        + allocatedContainer.getNodeId().getPort() + ", containerNodeURI="
                        + allocatedContainer.getNodeHttpAddress() + ", containerResourceMemory"
                        + allocatedContainer.getResource().getMemory());

                LaunchAsterixContainer runnableLaunchContainer = new LaunchAsterixContainer(allocatedContainer,
                        containerListener);
                Thread launchThread = new Thread(runnableLaunchContainer, "Asterix CC/NC");

                // I want to know if this node is the CC, because it must start before the NCs. 
                LOG.info("Allocated: " + allocatedContainer.getNodeId().getHost());
                LOG.info("CC : " + cC.getId());
                synchronized(pendingNCs){
                    try {
                        if (ccUp.get()) {
                            pendingNCs.remove(containerToNode(allocatedContainer, clusterDesc));
                        }
                    } catch(UnknownHostException ex){
                        LOG.error("Unknown host allocated for us by RM- this shouldn't happen.", ex);
                    }
                }

                if (containerIsCC(allocatedContainer)) {
                    ccUp.set(true);
                }
                // launch and start the container on a separate thread to keep
                // the main thread unblocked
                // as all containers may not be allocated at one go.
                launchThreads.add(launchThread);
                launchThread.start();
            }
        }

        /**
         * Ask the processes on the container to gracefully exit.
         */
        public void onShutdownRequest() {
            LOG.info("AM shutting down per request");
            done = true;
        }

        public void onNodesUpdated(List<NodeReport> updatedNodes) {
            //TODO: This will become important when we deal with what happens if an NC dies
        }

        public float getProgress() {
            //return half way because progress is basically meaningless for us
            if (!doneAllocating) {
                return 0.0f;
            }
            return (float) 0.5;
        }

        public void onError(Throwable arg0) {
            LOG.error("Fatal Error recieved by AM: " + arg0);
            done = true;
        }
    }

    private class NMCallbackHandler implements NMClientAsync.CallbackHandler {

        private ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<ContainerId, Container>();

        public void addContainer(ContainerId containerId, Container container) {
            containers.putIfAbsent(containerId, container);
        }

        public void onContainerStopped(ContainerId containerId) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Succeeded to stop Container " + containerId);
            }
            containers.remove(containerId);
        }

        public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Container Status: id=" + containerId + ", status=" + containerStatus);
            }
        }

        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Succeeded to start Container " + containerId);
            }
            Container container = containers.get(containerId);
            if (container != null) {
                nmClientAsync.getContainerStatusAsync(containerId, container.getNodeId());
            }
        }

        public void onStartContainerError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to start Container " + containerId);
            containers.remove(containerId);
        }

        public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to query the status of Container " + containerId);
        }

        public void onStopContainerError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to stop Container " + containerId);
            containers.remove(containerId);
        }
    }

    /**
     * Thread to connect to the {@link ContainerManagementProtocol} and launch the container
     * that will execute the shell command.
     */
    private class LaunchAsterixContainer implements Runnable {

        // Allocated container
        final Container container;

        final NMCallbackHandler containerListener;

        /**
         * @param lcontainer
         *            Allocated container
         * @param containerListener
         *            Callback handler of the container
         */
        public LaunchAsterixContainer(Container lcontainer, NMCallbackHandler containerListener) {
            this.container = lcontainer;
            this.containerListener = containerListener;
        }

        /**
         * Connects to CM, sets up container launch context
         * for shell command and eventually dispatches the container
         * start request to the CM.
         */
        public void run() {
            LOG.info("Setting up container launch container for containerid=" + container.getId());
            ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
            // Set the local resources
            ctx.setLocalResources(localResources);

            //Set the env variables to be setup in the env where the application master will be run
            LOG.info("Set the environment for the node");
            Map<String, String> env = new HashMap<String, String>();

            // Add AppMaster.jar location to classpath
            // At some point we should not be required to add
            // the hadoop specific classpaths to the env.
            // It should be provided out of the box.
            // For now setting all required classpaths including
            // the classpath to "." for the application jar
            StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$()).append(File.pathSeparatorChar)
                    .append("." + File.pathSeparatorChar + "*");
            for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                    YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
                classPathEnv.append(File.pathSeparatorChar);
                classPathEnv.append(c.trim());
            }
            classPathEnv.append('.').append(File.pathSeparatorChar).append("log4j.properties");

            // add the runtime classpath needed for tests to work
            if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
                classPathEnv.append(System.getProperty("path.separator"));
                classPathEnv.append(System.getProperty("java.class.path"));
                env.put("HADOOP_CONF_DIR", System.getProperty("user.dir") + File.separator + "target" + File.separator);
            }

            env.put("CLASSPATH", classPathEnv.toString());

            ctx.setEnvironment(env);
            LOG.info(ctx.getEnvironment().toString());
            List<String> startCmd = null;
            if (obliterate) {
                LOG.debug("AM in obliterate mode");
                startCmd = produceObliterateCommand(container);
            } else if (backup) {
                startCmd = produceBackupCommand(container);
                LOG.debug("AM in backup mode");
            } else if (restore) {
                startCmd = produceRestoreCommand(container);
                LOG.debug("AM in restore mode");
            } else {
                startCmd = produceStartCmd(container);
            }

            if (startCmd == null || startCmd.size() == 0) {
                LOG.fatal("Could not map one or more NCs to NM container hosts- aborting!");
                return;
            }

            for (String s : startCmd) {
                LOG.info("Command to execute: " + s);
            }
            ctx.setCommands(startCmd);
            containerListener.addContainer(container.getId(), container);
            //finally start the container!?
            nmClientAsync.startContainerAsync(container, ctx);
        }

        /**
         * Determines for a given container what the necessary command line
         * arguments are to start the Asterix processes on that instance
         * 
         * @param container
         *            The container to produce the commands for
         * @return A list of the commands that should be executed
         */
        private List<String> produceStartCmd(Container container) {
            List<String> commands = new ArrayList<String>();
            // Set the necessary command to execute on the allocated container
            List<CharSequence> vargs = new ArrayList<CharSequence>(5);

            vargs.add(JAVA_HOME + File.separator + "bin" + File.separator + "java");
            vargs.add("-classpath " + '\'' + ASTERIX_ZIP_NAME + File.separator + "repo" + File.separator + "*\'");
            vargs.add("-Dapp.repo=" + ASTERIX_ZIP_NAME + File.separator + "repo" + File.separator);
            //first see if this node is the CC
            if (containerIsCC(container) && (ccStarted.get() == false)) {
                LOG.info("CC found on container" + container.getNodeId().getHost());
                //get our java opts
                vargs.add(ccJavaOpts);
                vargs.add(CC_CLASSNAME);
                vargs.add("-app-cc-main-class edu.uci.ics.asterix.hyracks.bootstrap.CCApplicationEntryPoint");
                vargs.add("-cluster-net-ip-address " + cC.getClusterIp());
                vargs.add("-client-net-ip-address " + cC.getClientIp());
                ccStarted.set(true);

            } else {
                //now we need to know what node we are on, so we can apply the correct properties

                Node local;
                try {
                    local = containerToNode(container, clusterDesc);
                    LOG.info("Attempting to start NC on host " + local.getId());
                    String iodevice = local.getIodevices();
                    if (iodevice == null) {
                        iodevice = clusterDesc.getIodevices();
                    }
                    String storageSuffix = local.getStore() == null ? clusterDesc.getStore() : local.getStore();
                    String storagePath = iodevice + File.separator + storageSuffix;
                    vargs.add(ncJavaOpts);
                    vargs.add(NC_CLASSNAME);
                    vargs.add("-app-nc-main-class edu.uci.ics.asterix.hyracks.bootstrap.NCApplicationEntryPoint");
                    vargs.add("-node-id " + local.getId());
                    vargs.add("-cc-host " + cC.getClusterIp());
                    vargs.add("-iodevices " + storagePath);
                    vargs.add("-cluster-net-ip-address " + local.getClusterIp());
                    vargs.add("-data-ip-address " + local.getClusterIp());
                    vargs.add("-result-ip-address " + local.getClusterIp());
                    vargs.add("--");
                    if(initial){
                        vargs.add("-initial-run ");
                    }
                } catch (UnknownHostException e) {
                    LOG.error("Unable to find NC or CC configured for host: " + container.getId() + " " + e);
                }
            }

            // Add log redirect params
            vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separator + "stdout");
            vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separator + "stderr");

            // Get final commmand
            StringBuilder command = new StringBuilder();
            for (CharSequence str : vargs) {
                command.append(str).append(" ");
            }
            commands.add(command.toString());
            LOG.error(Arrays.toString(commands.toArray()));
            return commands;
        }

        private List<String> produceObliterateCommand(Container container) {
            //if this container has no NCs on it, nothing will be there to delete. 
            Node local = null;
            List<String> iodevices = null;
            try {
                local = containerToNode(container, clusterDesc);
                if (local.getIodevices() == null) {
                    iodevices = Arrays.asList(clusterDesc.getIodevices().split(",", -1));
                } else {
                    iodevices = Arrays.asList(local.getIodevices().split(",", -1));
                }
            } catch (UnknownHostException e) {
                //we expect this may happen for the CC if it isn't colocated with an NC. otherwise it is not suppose to happen.
                if (!containerIsCC(container)) {
                    LOG.error("Unable to find NC configured for host: " + container.getId() + e);
                    return null;
                }
                else {
                    return Arrays.asList("");
                }
            }
            StringBuilder classPathEnv = new StringBuilder("").append("*");
            classPathEnv.append(File.pathSeparatorChar).append("log4j.properties");

            List<String> commands = new ArrayList<String>();
            Vector<CharSequence> vargs = new Vector<CharSequence>(5);
            vargs.add(JAVA_HOME + File.separator + "bin" + File.separator + "java");
            vargs.add("-cp " + classPathEnv.toString());
            vargs.add(OBLITERATOR_CLASSNAME);
            for (String s : iodevices) {
                vargs.add(s + File.separator + clusterDesc.getStore());
                LOG.debug("Deleting from: " + s);
                //logs only exist on 1st iodevice
                if (iodevices.indexOf(s) == 0) {
                    vargs.add(clusterDesc.getTxnLogDir() + "txnLogs" + File.separator);
                    LOG.debug("Deleting logs from: " + clusterDesc.getTxnLogDir());
                }
            }
            vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separator + "stdout");
            vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separator + "stderr");
            StringBuilder command = new StringBuilder();
            for (CharSequence str : vargs) {
                command.append(str).append(" ");
            }
            commands.add(command.toString());
            return commands;
        }

        private List<String> produceBackupCommand(Container container) {
            Node local = null;
            List<String> iodevices = null;
            try {
                local = containerToNode(container, clusterDesc);
                if (local.getIodevices() == null) {
                    iodevices = Arrays.asList(clusterDesc.getIodevices().split(",", -1));
                } else {
                    iodevices = Arrays.asList(local.getIodevices().split(",", -1));
                }
            } catch (UnknownHostException e) {
                //we expect this may happen for the CC if it isn't colocated with an NC. otherwise it is not suppose to happen.
                if (!containerIsCC(container)) {
                    LOG.error("Unable to find NC configured for host: " + container.getId() + e);
                    return null;
                }else {
                    return Arrays.asList("");
                }
            }
            StringBuilder classPathEnv = new StringBuilder("").append("." + File.separator + "*");
            for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                    YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
                classPathEnv.append(File.pathSeparatorChar);
                classPathEnv.append(c.trim());
            }
            classPathEnv.append(File.pathSeparatorChar).append("." + File.separator + "log4j.properties");

            List<String> commands = new ArrayList<String>();
            Vector<CharSequence> vargs = new Vector<CharSequence>(5);
            vargs.add(JAVA_HOME + File.separator + "bin" + File.separator + "java");
            vargs.add("-cp " + classPathEnv.toString());
            vargs.add(HDFS_BACKUP_CLASSNAME);
            vargs.add("-backup");

            String dstBase = instanceConfPath + "backups" + Path.SEPARATOR + backupTimestamp + Path.SEPARATOR
                    + local.getId();
            try {
                createBackupFolder(dstBase);
            } catch (IOException e) {
                //something very bad happened- return null to cause attempt to abort
                return null;
            }
            for (String s : iodevices) {
                List<String> ioComponents = Arrays.asList(s.split("\\/"));
                StringBuilder dst = new StringBuilder().append(dstBase);
                for (String io : ioComponents) {
                    dst.append(io);
                    if (ioComponents.indexOf(io) != ioComponents.size() - 1) {
                        dst.append("_");
                    }
                }
                dst.append(Path.SEPARATOR);
                vargs.add(s + File.separator + clusterDesc.getStore() + "," + dst);
                LOG.debug("Backing up from: " + s);
                //logs only exist on 1st iodevice
                if (iodevices.indexOf(s) == 0) {
                    LOG.debug("Backing up logs from: " + clusterDesc.getTxnLogDir());
                    vargs.add(clusterDesc.getTxnLogDir() + "txnLogs" + File.separator + "," + dst);
                }
            }
            LOG.debug("Backing up to: " + instanceConfPath + "backups" + Path.SEPARATOR + local.getId());

            vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separator + "stdout");
            vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separator + "stderr");
            StringBuilder command = new StringBuilder();
            for (CharSequence str : vargs) {
                command.append(str).append(" ");
            }
            commands.add(command.toString());
            return commands;
        }

        private void createBackupFolder(String path) throws IOException {
            FileSystem fs = FileSystem.get(conf);
            Path backupFolder = new Path(path);
            fs.mkdirs(backupFolder);
        }

        private List<String> produceRestoreCommand(Container container) {
            if (containerIsCC(container)) {
                List<String> blank = new ArrayList<String>();
                blank.add("");
                return blank;
            }
            Node local = null;
            List<String> iodevices = null;
            try {
                local = containerToNode(container, clusterDesc);
                if (local.getIodevices() == null) {
                    iodevices = Arrays.asList(clusterDesc.getIodevices().split(",", -1));
                } else {
                    iodevices = Arrays.asList(local.getIodevices().split(",", -1));
                }
            } catch (UnknownHostException e) {
                //we expect this may happen for the CC if it isn't colocated with an NC. otherwise it is not suppose to happen.
                if (!containerIsCC(container)) {
                    LOG.error("Unable to find NC configured for host: " + container.getId() + e);
                    return null;
                } else {
                    return Arrays.asList("");
                }
            }
            StringBuilder classPathEnv = new StringBuilder("").append("." + File.separator + "*");
            for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                    YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
                classPathEnv.append(File.pathSeparatorChar);
                classPathEnv.append(c.trim());
            }
            classPathEnv.append(File.pathSeparatorChar).append("." + File.separator + "log4j.properties");

            List<String> commands = new ArrayList<String>();
            Vector<CharSequence> vargs = new Vector<CharSequence>(5);
            vargs.add(JAVA_HOME + File.separator + "bin" + File.separator + "java");
            vargs.add("-cp " + classPathEnv.toString());
            vargs.add(HDFS_BACKUP_CLASSNAME);
            vargs.add("-restore");
            String srcBase = instanceConfPath + "backups" + Path.SEPARATOR + Long.parseLong(snapName) + Path.SEPARATOR
                    + local.getId();
            for (String s : iodevices) {
                List<String> ioComponents = Arrays.asList(s.split("\\/"));
                StringBuilder src = new StringBuilder().append(srcBase);
                for (String io : ioComponents) {
                    src.append(io);
                    if (ioComponents.indexOf(io) != ioComponents.size() - 1) {
                        src.append("_");
                    }
                }
                src.append(Path.SEPARATOR);
                try {
                    FileSystem fs = FileSystem.get(conf);
                    FileStatus[] backups = fs.listStatus(new Path(src.toString()));
                    for (FileStatus b : backups) {
                        if (!b.getPath().toString().contains("txnLogs")
                                && !b.getPath().toString().contains(File.separator + "asterix_root_metadata")) {
                            vargs.add(b.getPath() + "," + s + File.separator + clusterDesc.getStore());
                        }
                    }
                } catch (IOException e) {
                    LOG.error("Could not stat backup directory in DFS");
                }
                vargs.add(src + "," + s + clusterDesc.getStore());
                LOG.debug("Restoring from: " + s);
                //logs only exist on 1st iodevice
                if (iodevices.indexOf(s) == 0) {
                    vargs.add(src + "txnLogs" + File.separator + "," + clusterDesc.getTxnLogDir() + File.separator);

                    LOG.debug("Restoring logs from: " + clusterDesc.getTxnLogDir());
                }
            }
            LOG.debug("Restoring to: " + instanceConfPath + "backups" + Path.SEPARATOR + local.getId());

            vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separator + "stdout");
            vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separator + "stderr");
            StringBuilder command = new StringBuilder();
            for (CharSequence str : vargs) {
                command.append(str).append(" ");
            }
            commands.add(command.toString());
            return commands;
        }

    }
}
