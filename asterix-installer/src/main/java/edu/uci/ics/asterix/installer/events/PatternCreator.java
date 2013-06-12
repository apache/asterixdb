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
package edu.uci.ics.asterix.installer.events;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.uci.ics.asterix.event.driver.EventDriver;
import edu.uci.ics.asterix.event.schema.cluster.Cluster;
import edu.uci.ics.asterix.event.schema.cluster.Node;
import edu.uci.ics.asterix.event.schema.pattern.Delay;
import edu.uci.ics.asterix.event.schema.pattern.Event;
import edu.uci.ics.asterix.event.schema.pattern.Nodeid;
import edu.uci.ics.asterix.event.schema.pattern.Pattern;
import edu.uci.ics.asterix.event.schema.pattern.Patterns;
import edu.uci.ics.asterix.event.schema.pattern.Value;
import edu.uci.ics.asterix.installer.command.BackupCommand;
import edu.uci.ics.asterix.installer.command.StopCommand;
import edu.uci.ics.asterix.installer.driver.InstallerDriver;
import edu.uci.ics.asterix.installer.driver.InstallerUtil;
import edu.uci.ics.asterix.installer.error.VerificationUtil;
import edu.uci.ics.asterix.installer.model.AsterixInstance;
import edu.uci.ics.asterix.installer.model.BackupInfo;
import edu.uci.ics.asterix.installer.model.BackupInfo.BackupType;
import edu.uci.ics.asterix.installer.schema.conf.Backup;
import edu.uci.ics.asterix.installer.service.ILookupService;
import edu.uci.ics.asterix.installer.service.ServiceProvider;

public class PatternCreator {

    private ILookupService lookupService = ServiceProvider.INSTANCE.getLookupService();

    private void addInitialDelay(Pattern p, int delay, String unit) {
        Delay d = new Delay(new Value(null, "" + delay), unit);
        p.setDelay(d);
    }

    public Patterns getAsterixBinaryTransferPattern(String asterixInstanceName, Cluster cluster) throws Exception {
        String ccLocationIp = cluster.getMasterNode().getClusterIp();
        String destDir = cluster.getWorkingDir().getDir() + File.separator + "asterix";
        List<Pattern> ps = new ArrayList<Pattern>();

        Pattern copyHyracks = createCopyHyracksPattern(asterixInstanceName, cluster, ccLocationIp, destDir);
        ps.add(copyHyracks);

        boolean copyHyracksToNC = !cluster.getWorkingDir().isNFS();

        for (Node node : cluster.getNode()) {
            if (copyHyracksToNC) {
                Pattern copyHyracksForNC = createCopyHyracksPattern(asterixInstanceName, cluster, node.getClusterIp(),
                        destDir);
                ps.add(copyHyracksForNC);
            }
        }
        ps.addAll(createHadoopLibraryTransferPattern(cluster).getPattern());
        Patterns patterns = new Patterns(ps);
        return patterns;
    }

    public Patterns getStartAsterixPattern(String asterixInstanceName, Cluster cluster) throws Exception {
        String ccLocationId = cluster.getMasterNode().getId();
        List<Pattern> ps = new ArrayList<Pattern>();

        Pattern createCC = createCCStartPattern(ccLocationId);
        addInitialDelay(createCC, 3, "sec");
        ps.add(createCC);

        for (Node node : cluster.getNode()) {
            String iodevices = node.getIodevices() == null ? cluster.getIodevices() : node.getIodevices();
            Pattern createNC = createNCStartPattern(cluster.getMasterNode().getClusterIp(), node.getId(),
                    asterixInstanceName + "_" + node.getId(), iodevices);
            addInitialDelay(createNC, 5, "sec");
            ps.add(createNC);
        }

        Patterns patterns = new Patterns(ps);
        return patterns;
    }

    public Patterns getStopCommandPattern(StopCommand stopCommand) throws Exception {
        List<Pattern> ps = new ArrayList<Pattern>();
        AsterixInstance asterixInstance = lookupService.getAsterixInstance(stopCommand.getAsterixInstanceName());
        Cluster cluster = asterixInstance.getCluster();

        String ccLocation = cluster.getMasterNode().getId();
        Pattern createCC = createCCStopPattern(ccLocation);
        addInitialDelay(createCC, 5, "sec");
        ps.add(createCC);

        String asterixInstanceName = stopCommand.getAsterixInstanceName();
        int nodeControllerIndex = 1;
        for (Node node : cluster.getNode()) {
            Pattern createNC = createNCStopPattern(node.getId(), asterixInstanceName + "_" + nodeControllerIndex);
            ps.add(createNC);
            nodeControllerIndex++;
        }

        Patterns patterns = new Patterns(ps);
        return patterns;
    }

    public Patterns getBackUpAsterixPattern(AsterixInstance instance, Backup backupConf) throws Exception {
        BackupType backupType = BackupInfo.getBackupType(backupConf);
        Patterns patterns = null;
        switch (backupType) {
            case HDFS:
                patterns = getHDFSBackUpAsterixPattern(instance, backupConf);
                break;
            case LOCAL:
                patterns = getLocalBackUpAsterixPattern(instance, backupConf);
                break;
        }
        return patterns;
    }

    public Patterns getRestoreAsterixPattern(AsterixInstance instance, BackupInfo backupInfo) throws Exception {
        BackupType backupType = backupInfo.getBackupType();
        Patterns patterns = null;
        switch (backupType) {
            case HDFS:
                patterns = getHDFSRestoreAsterixPattern(instance, backupInfo);
                break;
            case LOCAL:
                patterns = getLocalRestoreAsterixPattern(instance, backupInfo);
                break;
        }
        return patterns;
    }

    private Patterns getHDFSBackUpAsterixPattern(AsterixInstance instance, Backup backupConf) throws Exception {
        Cluster cluster = instance.getCluster();
        String hdfsUrl = backupConf.getHdfs().getUrl();
        String hadoopVersion = backupConf.getHdfs().getVersion();
        String hdfsBackupDir = backupConf.getBackupDir();
        VerificationUtil.verifyBackupRestoreConfiguration(hdfsUrl, hadoopVersion, hdfsBackupDir);
        String workingDir = cluster.getWorkingDir().getDir();
        String backupId = "" + instance.getBackupInfo().size();
        String store;
        String pargs;
        String iodevices;
        List<Pattern> patternList = new ArrayList<Pattern>();
        for (Node node : cluster.getNode()) {
            Nodeid nodeid = new Nodeid(new Value(null, node.getId()));
            iodevices = node.getIodevices() == null ? instance.getCluster().getIodevices() : node.getIodevices();
            store = node.getStore() == null ? cluster.getStore() : node.getStore();
            pargs = workingDir + " " + instance.getName() + " " + iodevices + " " + store + " "
                    + BackupCommand.ASTERIX_ROOT_METADATA_DIR + " " + InstallerUtil.TXN_LOG_DIR + " " + backupId + " "
                    + hdfsBackupDir + " " + "hdfs" + " " + node.getId() + " " + hdfsUrl + " " + hadoopVersion;
            Event event = new Event("backup", nodeid, pargs);
            patternList.add(new Pattern(null, 1, null, event));
        }
        return new Patterns(patternList);
    }

    private Patterns getLocalBackUpAsterixPattern(AsterixInstance instance, Backup backupConf) throws Exception {
        Cluster cluster = instance.getCluster();
        String backupDir = backupConf.getBackupDir();
        String workingDir = cluster.getWorkingDir().getDir();
        String backupId = "" + instance.getBackupInfo().size();
        String iodevices;
        String txnLogDir;
        String store;
        String pargs;
        List<Pattern> patternList = new ArrayList<Pattern>();
        for (Node node : cluster.getNode()) {
            Nodeid nodeid = new Nodeid(new Value(null, node.getId()));
            iodevices = node.getIodevices() == null ? instance.getCluster().getIodevices() : node.getIodevices();
            txnLogDir = node.getTxnLogDir() == null ? instance.getCluster().getTxnLogDir() : node.getTxnLogDir();
            store = node.getStore() == null ? cluster.getStore() : node.getStore();
            pargs = workingDir + " " + instance.getName() + " " + iodevices + " " + store + " "
                    + BackupCommand.ASTERIX_ROOT_METADATA_DIR  + " " + txnLogDir + " " + backupId + " " + backupDir
                    + " " + "local" + " " + node.getId();
            Event event = new Event("backup", nodeid, pargs);
            patternList.add(new Pattern(null, 1, null, event));
        }
        return new Patterns(patternList);
    }

    public Patterns getHDFSRestoreAsterixPattern(AsterixInstance instance, BackupInfo backupInfo) throws Exception {
        Cluster cluster = instance.getCluster();
        String clusterStore = instance.getCluster().getStore();
        String hdfsUrl = backupInfo.getBackupConf().getHdfs().getUrl();
        String hadoopVersion = backupInfo.getBackupConf().getHdfs().getVersion();
        String hdfsBackupDir = backupInfo.getBackupConf().getBackupDir();
        VerificationUtil.verifyBackupRestoreConfiguration(hdfsUrl, hadoopVersion, hdfsBackupDir);
        String workingDir = cluster.getWorkingDir().getDir();
        int backupId = backupInfo.getId();
        String nodeStore;
        String pargs;
        List<Pattern> patternList = new ArrayList<Pattern>();
        for (Node node : cluster.getNode()) {
            Nodeid nodeid = new Nodeid(new Value(null, node.getId()));
            String iodevices = node.getIodevices() == null ? cluster.getIodevices() : node.getIodevices();
            nodeStore = node.getStore() == null ? clusterStore : node.getStore();
            pargs = workingDir + " " + instance.getName() + " " + iodevices + " " + nodeStore + " "
                    + BackupCommand.ASTERIX_ROOT_METADATA_DIR + " " + InstallerUtil.TXN_LOG_DIR + " " + backupId + " "
                    + " " + hdfsBackupDir + " " + "hdfs" + " " + node.getId() + " " + hdfsUrl + " " + hadoopVersion;
            Event event = new Event("restore", nodeid, pargs);
            patternList.add(new Pattern(null, 1, null, event));
        }
        return new Patterns(patternList);
    }

    public Patterns getLocalRestoreAsterixPattern(AsterixInstance instance, BackupInfo backupInfo) throws Exception {
        Cluster cluster = instance.getCluster();
        String clusterStore = instance.getCluster().getStore();
        String backupDir = backupInfo.getBackupConf().getBackupDir();
        String workingDir = cluster.getWorkingDir().getDir();
        int backupId = backupInfo.getId();
        String nodeStore;
        String pargs;
        List<Pattern> patternList = new ArrayList<Pattern>();
        for (Node node : cluster.getNode()) {
            Nodeid nodeid = new Nodeid(new Value(null, node.getId()));
            String iodevices = node.getIodevices() == null ? cluster.getIodevices() : node.getIodevices();
            nodeStore = node.getStore() == null ? clusterStore : node.getStore();
            pargs = workingDir + " " + instance.getName() + " " + iodevices + " " + nodeStore + " "
                    + BackupCommand.ASTERIX_ROOT_METADATA_DIR + " " + InstallerUtil.TXN_LOG_DIR + " " + backupId + " "
                    + backupDir + " " + "local" + " " + node.getId();
            Event event = new Event("restore", nodeid, pargs);
            patternList.add(new Pattern(null, 1, null, event));
        }
        return new Patterns(patternList);
    }

    public Patterns createHadoopLibraryTransferPattern(Cluster cluster) throws Exception {
        List<Pattern> patternList = new ArrayList<Pattern>();
        String workingDir = cluster.getWorkingDir().getDir();
        String hadoopVersion = InstallerDriver.getConfiguration().getBackup().getHdfs().getVersion();
        File hadoopDir = new File(InstallerDriver.getManagixHome() + File.separator
                + InstallerDriver.MANAGIX_INTERNAL_DIR + File.separator + "hadoop-" + hadoopVersion);
        if (!hadoopDir.exists()) {
            throw new IllegalStateException("Hadoop version :" + hadoopVersion + " not supported");
        }

        Nodeid nodeid = new Nodeid(new Value(null, EventDriver.CLIENT_NODE.getId()));
        String username = cluster.getUsername() != null ? cluster.getUsername() : System.getProperty("user.name");
        String pargs = username + " " + hadoopDir.getAbsolutePath() + " " + cluster.getMasterNode().getClusterIp()
                + " " + workingDir;
        Event event = new Event("directory_transfer", nodeid, pargs);
        Pattern p = new Pattern(null, 1, null, event);
        addInitialDelay(p, 2, "sec");
        patternList.add(p);

        boolean copyToNC = !cluster.getWorkingDir().isNFS();
        if (copyToNC) {
            for (Node node : cluster.getNode()) {
                nodeid = new Nodeid(new Value(null, node.getId()));
                pargs = cluster.getUsername() + " " + hadoopDir.getAbsolutePath() + " " + node.getClusterIp() + " "
                        + workingDir;
                event = new Event("directory_transfer", nodeid, pargs);
                p = new Pattern(null, 1, null, event);
                addInitialDelay(p, 2, "sec");
                patternList.add(p);
            }
        }
        Patterns patterns = new Patterns(patternList);
        return patterns;
    }

    public Patterns createDeleteInstancePattern(AsterixInstance instance) throws Exception {
        List<Pattern> patternList = new ArrayList<Pattern>();
        patternList.addAll(createRemoveAsterixStoragePattern(instance).getPattern());
        if (instance.getBackupInfo() != null && instance.getBackupInfo().size() > 0) {
            List<BackupInfo> backups = instance.getBackupInfo();
            Set<String> removedBackupDirsHDFS = new HashSet<String>();
            Set<String> removedBackupDirsLocal = new HashSet<String>();

            String backupDir;
            for (BackupInfo binfo : backups) {
                backupDir = binfo.getBackupConf().getBackupDir();
                switch (binfo.getBackupType()) {
                    case HDFS:
                        if (removedBackupDirsHDFS.contains(backups)) {
                            continue;
                        }
                        patternList.addAll(createRemoveHDFSBackupPattern(instance, backupDir).getPattern());
                        removedBackupDirsHDFS.add(backupDir);
                        break;

                    case LOCAL:
                        if (removedBackupDirsLocal.contains(backups)) {
                            continue;
                        }
                        patternList.addAll(createRemoveLocalBackupPattern(instance, backupDir).getPattern());
                        removedBackupDirsLocal.add(backupDir);
                        break;
                }

            }
        }
        patternList.addAll(createRemoveAsterixLogDirPattern(instance).getPattern());
        patternList.addAll(createRemoveAsterixRootMetadata(instance).getPattern());
        patternList.addAll(createRemoveAsterixTxnLogs(instance).getPattern());
        Patterns patterns = new Patterns(patternList);
        return patterns;
    }

    private Patterns createRemoveAsterixTxnLogs(AsterixInstance instance) throws Exception {
        List<Pattern> patternList = new ArrayList<Pattern>();
        Cluster cluster = instance.getCluster();
        Nodeid nodeid = null;
        Event event = null;
        for (Node node : cluster.getNode()) {
            String txnLogDir = node.getTxnLogDir() == null ? cluster.getTxnLogDir() : node.getTxnLogDir();
            nodeid = new Nodeid(new Value(null, node.getId()));
            event = new Event("file_delete", nodeid, txnLogDir);
            patternList.add(new Pattern(null, 1, null, event));
        }

        Patterns patterns = new Patterns(patternList);
        return patterns;
    }

    private Patterns createRemoveHDFSBackupPattern(AsterixInstance instance, String hdfsBackupDir) throws Exception {
        List<Pattern> patternList = new ArrayList<Pattern>();
        Cluster cluster = instance.getCluster();
        String hdfsUrl = InstallerDriver.getConfiguration().getBackup().getHdfs().getUrl();
        String hadoopVersion = InstallerDriver.getConfiguration().getBackup().getHdfs().getVersion();
        String workingDir = cluster.getWorkingDir().getDir();
        Node launchingNode = cluster.getNode().get(0);
        Nodeid nodeid = new Nodeid(new Value(null, launchingNode.getId()));
        String pathToDelete = hdfsBackupDir + File.separator + instance.getName();
        String pargs = workingDir + " " + hadoopVersion + " " + hdfsUrl + " " + pathToDelete;
        Event event = new Event("hdfs_delete", nodeid, pargs);
        patternList.add(new Pattern(null, 1, null, event));
        Patterns patterns = new Patterns(patternList);
        return patterns;
    }

    private Patterns createRemoveLocalBackupPattern(AsterixInstance instance, String localBackupDir) throws Exception {
        List<Pattern> patternList = new ArrayList<Pattern>();
        Cluster cluster = instance.getCluster();

        String pathToDelete = localBackupDir + File.separator + instance.getName();
        String pargs = pathToDelete;
        List<String> removedBackupDirs = new ArrayList<String>();
        for (Node node : cluster.getNode()) {
            if (removedBackupDirs.contains(node.getClusterIp())) {
                continue;
            }
            Nodeid nodeid = new Nodeid(new Value(null, node.getId()));
            Event event = new Event("file_delete", nodeid, pargs);
            patternList.add(new Pattern(null, 1, null, event));
            removedBackupDirs.add(node.getClusterIp());
        }

        Patterns patterns = new Patterns(patternList);
        return patterns;
    }

    public Patterns createRemoveAsterixWorkingDirPattern(AsterixInstance instance) throws Exception {
        List<Pattern> patternList = new ArrayList<Pattern>();
        Cluster cluster = instance.getCluster();
        String workingDir = cluster.getWorkingDir().getDir();
        String pargs = workingDir;
        Nodeid nodeid = new Nodeid(new Value(null, cluster.getMasterNode().getId()));
        Event event = new Event("file_delete", nodeid, pargs);
        patternList.add(new Pattern(null, 1, null, event));

        if (!cluster.getWorkingDir().isNFS()) {
            for (Node node : cluster.getNode()) {
                nodeid = new Nodeid(new Value(null, node.getId()));
                event = new Event("file_delete", nodeid, pargs);
                patternList.add(new Pattern(null, 1, null, event));
            }
        }
        Patterns patterns = new Patterns(patternList);
        return patterns;
    }

    private Patterns createRemoveAsterixRootMetadata(AsterixInstance instance) throws Exception {
        List<Pattern> patternList = new ArrayList<Pattern>();
        Cluster cluster = instance.getCluster();
        Nodeid nodeid = null;
        String pargs = null;
        Event event = null;
        for (Node node : cluster.getNode()) {
            String iodevices = node.getIodevices() == null ? cluster.getIodevices() : node.getIodevices();
            String primaryIODevice = iodevices.split(",")[0].trim();
            pargs = primaryIODevice + File.separator + BackupCommand.ASTERIX_ROOT_METADATA_DIR;
            nodeid = new Nodeid(new Value(null, node.getId()));
            event = new Event("file_delete", nodeid, pargs);
            patternList.add(new Pattern(null, 1, null, event));
        }

        Patterns patterns = new Patterns(patternList);
        return patterns;
    }

    private Patterns createRemoveAsterixLogDirPattern(AsterixInstance instance) throws Exception {
        List<Pattern> patternList = new ArrayList<Pattern>();
        Cluster cluster = instance.getCluster();
        String pargs = instance.getCluster().getLogDir();
        Nodeid nodeid = new Nodeid(new Value(null, cluster.getMasterNode().getId()));
        Event event = new Event("file_delete", nodeid, pargs);
        patternList.add(new Pattern(null, 1, null, event));

        for (Node node : cluster.getNode()) {
            nodeid = new Nodeid(new Value(null, node.getId()));
            event = new Event("file_delete", nodeid, pargs);
            patternList.add(new Pattern(null, 1, null, event));
        }

        Patterns patterns = new Patterns(patternList);
        return patterns;
    }

    private Patterns createRemoveAsterixStoragePattern(AsterixInstance instance) throws Exception {
        List<Pattern> patternList = new ArrayList<Pattern>();
        Cluster cluster = instance.getCluster();
        String pargs = null;

        for (Node node : cluster.getNode()) {
            Nodeid nodeid = new Nodeid(new Value(null, node.getId()));
            String[] nodeIODevices;
            String iodevices = node.getIodevices() == null ? cluster.getIodevices() : node.getIodevices();
            nodeIODevices = iodevices.trim().split(",");
            for (String nodeIODevice : nodeIODevices) {
                String nodeStore = node.getStore() == null ? cluster.getStore() : node.getStore();
                pargs = nodeIODevice.trim() + File.separator + nodeStore;
                Event event = new Event("file_delete", nodeid, pargs);
                patternList.add(new Pattern(null, 1, null, event));
            }
        }
        Patterns patterns = new Patterns(patternList);
        return patterns;
    }

    private Pattern createCopyHyracksPattern(String instanceName, Cluster cluster, String destinationIp, String destDir) {
        Nodeid nodeid = new Nodeid(new Value(null, EventDriver.CLIENT_NODE.getId()));
        String username = cluster.getUsername() != null ? cluster.getUsername() : System.getProperty("user.name");
        String asterixZipName = InstallerDriver.getAsterixZip().substring(
                InstallerDriver.getAsterixZip().lastIndexOf(File.separator) + 1);
        String fileToTransfer = new File(InstallerDriver.getAsterixDir() + File.separator + instanceName
                + File.separator + asterixZipName).getAbsolutePath();
        String pargs = username + " " + fileToTransfer + " " + destinationIp + " " + destDir + " " + "unpack";
        Event event = new Event("file_transfer", nodeid, pargs);
        return new Pattern(null, 1, null, event);
    }

    private Pattern createCCStartPattern(String hostId) {
        Nodeid nodeid = new Nodeid(new Value(null, hostId));
        Event event = new Event("cc_start", nodeid, "");
        return new Pattern(null, 1, null, event);
    }

    public Pattern createCCStopPattern(String hostId) {
        Nodeid nodeid = new Nodeid(new Value(null, hostId));
        Event event = new Event("cc_failure", nodeid, null);
        return new Pattern(null, 1, null, event);
    }

    public Pattern createNCStartPattern(String ccHost, String hostId, String nodeControllerId, String iodevices) {
        Nodeid nodeid = new Nodeid(new Value(null, hostId));
        String pargs = ccHost + " " + nodeControllerId + " " + iodevices;
        Event event = new Event("node_join", nodeid, pargs);
        return new Pattern(null, 1, null, event);
    }

    public Pattern createNCStopPattern(String hostId, String nodeControllerId) {
        Nodeid nodeid = new Nodeid(new Value(null, hostId));
        Event event = new Event("node_failure", nodeid, nodeControllerId);
        return new Pattern(null, 1, null, event);
    }

    public Patterns getGenerateLogPattern(String asterixInstanceName, Cluster cluster, String outputDir) {
        List<Pattern> patternList = new ArrayList<Pattern>();
        Map<String,String> nodeLogs = new HashMap<String,String>();
        
        String username = cluster.getUsername() == null ? System.getProperty("user.name") : cluster.getUsername();
        String srcHost = cluster.getMasterNode().getClientIp();
        Nodeid nodeid = new Nodeid(new Value(null, EventDriver.CLIENT_NODE.getId()));
        String srcDir = cluster.getMasterNode().getLogDir() == null ? cluster.getLogDir() : cluster.getMasterNode()
                .getLogDir();
        String destDir = outputDir + File.separator + "cc";
        String pargs = username + " " + srcHost + " " + srcDir + " " + destDir;
        Event event = new Event("directory_copy", nodeid, pargs);
        Pattern p = new Pattern(null, 1, null, event);
        patternList.add(p);
        nodeLogs.put(cluster.getMasterNode().getClusterIp(),srcDir);
        for (Node node : cluster.getNode()) {
            srcHost = node.getClusterIp();
            srcDir = node.getLogDir() == null ? cluster.getLogDir() : node.getLogDir();
            if(nodeLogs.get(node.getClusterIp()) != null && nodeLogs.get(node.getClusterIp()).equals(srcDir)){
                continue;
            }
            destDir = outputDir + File.separator + node.getId();
            pargs = username + " " + srcHost +  " " + srcDir + " "  + destDir;
            event = new Event("directory_copy", nodeid, pargs);
            p = new Pattern(null, 1, null, event);
            patternList.add(p);
        }
        Patterns patterns = new Patterns(patternList);
        return patterns;
    }

}
