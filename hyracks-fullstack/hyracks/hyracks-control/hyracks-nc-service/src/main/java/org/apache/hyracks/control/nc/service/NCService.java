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
package org.apache.hyracks.control.nc.service;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.SystemUtils;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.control.common.config.ConfigUtils;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.common.controllers.ServiceConstants;
import org.apache.hyracks.control.common.controllers.ServiceConstants.ServiceCommand;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ini4j.Ini;
import org.kohsuke.args4j.CmdLineParser;

/**
 * Stand-alone process which listens for configuration information from the
 * CC and starts an NC. Intended to be a constantly-running service.
 */
public class NCService {

    private static final Logger LOGGER = LogManager.getLogger();

    /**
     * The .ini read from the CC (*not* the ncservice.ini file)
     */
    private static Ini ini = new Ini();

    /**
     * ID of *this* NC
     */
    private static String ncId = "";

    /**
     * The Ini section representing *this* NC
     */
    private static String nodeSection = null;

    /**
     * The NCServiceConfig
     */
    private static NCServiceConfig config;

    /**
     * The child Process, if one is active
     */
    private static Process proc = null;

    /**
     * The management bean for obtaining settings of the underlying operating system and hardware.
     */
    private static OperatingSystemMXBean osMXBean;

    private static List<String> buildCommand() throws IOException {
        List<String> cList = new ArrayList<>();

        // Find the command to run. For now, we allow overriding the name, but
        // still assume it's located in the bin/ directory of the deployment.
        // Even this is likely more configurability than we need.
        String command = ConfigUtils.getString(ini, nodeSection, NCConfig.Option.COMMAND.ini(), "hyracksnc");
        // app.home is specified by the Maven appassembler plugin. If it isn't set,
        // fall back to user's home dir. Again this is likely more flexibility
        // than we need.
        String apphome = System.getProperty("app.home", System.getProperty("user.home"));
        String path = apphome + File.separator + "bin" + File.separator;
        if (SystemUtils.IS_OS_WINDOWS) {
            cList.add(path + command + ".bat");
        } else {
            cList.add(path + command);
        }

        cList.add("-config-file");
        // Store the Ini file from the CC locally so NCConfig can read it.
        File tempIni = File.createTempFile("ncconf", ".conf");
        tempIni.deleteOnExit();

        ini.store(tempIni);
        cList.add(tempIni.getCanonicalPath());

        // pass in the PID of the NCService
        cList.add("-ncservice-pid");
        cList.add(System.getProperty("app.pid", "0"));
        return cList;
    }

    private static void configEnvironment(Map<String, String> env) {
        String jvmargs = ConfigUtils.getString(ini, nodeSection, NCConfig.Option.JVM_ARGS.ini(), null);
        if (jvmargs != null) {
            LOGGER.info("Using JAVA_OPTS from conf file (jvm.args)");
        } else {
            jvmargs = env.get("JAVA_OPTS");
            if (jvmargs != null) {
                LOGGER.info("Using JAVA_OPTS from environment");
            } else {
                LOGGER.info("Using default JAVA_OPTS");
                jvmargs = "";
            }
        }

        // Sets up memory parameter if it is not specified.
        if (!jvmargs.contains("-Xmx")) {
            long ramSize = ((com.sun.management.OperatingSystemMXBean) osMXBean).getTotalPhysicalMemorySize();
            int proportionalRamSize = (int) Math.ceil(0.6 * ramSize / (1024 * 1024));
            //if under 32bit JVM, use less than 1GB heap by default. otherwise use proportional ramsize.
            int heapSize = "32".equals(System.getProperty("sun.arch.data.model"))
                    ? (proportionalRamSize <= 1024 ? proportionalRamSize : 1024) : proportionalRamSize;
            jvmargs = jvmargs + " -Xmx" + heapSize + "m";
        }
        env.put("JAVA_OPTS", jvmargs.trim());
        LOGGER.info("Setting JAVA_OPTS to " + jvmargs);
    }

    /**
     * Attempts to launch the "real" NCDriver, based on the configuration
     * information gathered so far.
     *
     * @return true if the process was successfully launched and has now
     *         exited with a 0 (normal) exit code. false if some configuration error
     *         prevented the process from being launched or the process returned
     *         a non-0 (abnormal) exit code.
     */
    private static boolean launchNCProcess() {
        try {
            ProcessBuilder pb = new ProcessBuilder(buildCommand());
            configEnvironment(pb.environment());
            // QQQ inheriting probably isn't right
            pb.inheritIO();

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Launching NCDriver process");
            }

            // Logfile
            if (!"-".equals(config.logdir)) {
                pb.redirectErrorStream(true);
                File log = new File(config.logdir);
                if (!log.mkdirs() && !log.isDirectory()) {
                    throw new IOException(config.logdir + ": cannot create");
                    // If the directory IS there, all is well
                }
                File logfile = new File(config.logdir, "nc-" + ncId + ".log");
                try (FileWriter writer = new FileWriter(logfile, true)) {
                    writer.write("---------------------\n");
                    writer.write(new Date() + "\n");
                    writer.write("---------------------\n");
                }
                pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logfile));
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Logging to " + logfile.getCanonicalPath());
                }
            }
            proc = pb.start();

            boolean waiting = true;
            int retval = 0;
            while (waiting) {
                try {
                    retval = proc.waitFor();
                    waiting = false;
                } catch (InterruptedException ignored) {
                }
            }
            LOGGER.info("NCDriver exited with return value " + retval);
            if (retval == 99) {
                LOGGER.info("Terminating NCService based on return value from NCDriver");
                exit(0);
            }
            return retval == 0;
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                StringWriter sw = new StringWriter();
                try {
                    ini.store(sw);
                    LOGGER.log(Level.ERROR, "Configuration from CC broken: \n" + sw.toString(), e);
                } catch (IOException e1) {
                    LOGGER.log(Level.ERROR, "Configuration from CC broken, failed to serialize", e1);
                }
            }
            return false;
        }
    }

    private static boolean acceptConnection(InputStream is) {
        // Simple on-wire protocol:
        // magic cookie (string)
        // either:
        //   START_NC, ini file
        // or:
        //   TERMINATE
        // If we see anything else or have any error, crap out and await a different connection.
        try {
            ObjectInputStream ois = new ObjectInputStream(is);
            String magic = ois.readUTF();
            if (!ServiceConstants.NC_SERVICE_MAGIC_COOKIE.equals(magic)) {
                LOGGER.error("Connection used incorrect magic cookie");
                return false;
            }
            switch (ServiceCommand.valueOf(ois.readUTF())) {
                case START_NC:
                    String iniString = ois.readUTF();
                    ini = new Ini(new StringReader(iniString));
                    ncId = ConfigUtils.getString(ini, Section.LOCALNC, NCConfig.Option.NODE_ID, "");
                    nodeSection = "nc/" + ncId;
                    return launchNCProcess();
                case TERMINATE:
                    LOGGER.info("Terminating NCService based on command from CC");
                    exit(0);
                    break;
            }
        } catch (Exception e) {
            LOGGER.log(Level.ERROR, "Error decoding connection from server", e);
        }
        return false;
    }

    @SuppressWarnings("squid:S1147") // call to System.exit()
    private static void exit(int exitCode) {
        LOGGER.info("JVM Exiting.. Bye!");
        System.exit(exitCode);
    }

    public static void main(String[] args) throws Exception {
        // Register a shutdown hook which will kill the NC if the NC Service is killed.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if (proc != null) {
                    proc.destroy();
                    try {
                        proc.waitFor();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        config = new NCServiceConfig();
        CmdLineParser cp = new CmdLineParser(config);
        try {
            cp.parseArgument(args);
        } catch (Exception e) {
            e.printStackTrace();
            cp.printUsage(System.err);
            System.exit(1);
        }
        config.loadConfigAndApplyDefaults();

        // Initializes the oxMXBean.
        osMXBean = ManagementFactory.getOperatingSystemMXBean();

        // For now we implement a trivial listener which just
        // accepts an IP/port combination from the CC. This could
        // be made more advanced in several ways depending on whether
        // we want to expand the functionality of this service.
        // For now this gets the job done, without radically changing
        // the NC itself so that Managix can continue to function.
        InetAddress addr = config.address == null ? null : InetAddress.getByName(config.address);
        int port = config.port;

        // Loop forever - the NCService will always return to "waiting for CC" state
        // when the child NC terminates for any reason.
        while (true) {
            try (ServerSocket listener = new ServerSocket(port, 5, addr)) {
                boolean launched = false;
                while (!launched) {
                    LOGGER.info("Waiting for connection from CC on " + (addr == null ? "*" : addr) + ":" + port);
                    try (Socket socket = listener.accept()) {
                        // QQQ Because acceptConnection() doesn't return if the
                        // service is started appropriately, the socket remains
                        // open but non-responsive.
                        launched = acceptConnection(socket.getInputStream());
                    }
                }
            }
        }
    }
}
