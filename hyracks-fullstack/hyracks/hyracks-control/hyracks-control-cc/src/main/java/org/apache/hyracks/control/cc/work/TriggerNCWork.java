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
package org.apache.hyracks.control.cc.work;

import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.work.AbstractWork;
import org.ini4j.Ini;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.StringWriter;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A work which is run at CC startup for each NC specified in the configuration file.
 * It contacts the NC service on each node and passes in the NC-specific configuration.
 */
public class TriggerNCWork extends AbstractWork {

    // This constant must match the corresponding constant in
    // hyracks-control/hyracks-nc-service/src/main/java/org/apache/hyracks/control/nc/service/NCService.java
    // I didn't want to introduce a Maven-level dependency on the
    // hyracks-nc-service package (or vice-versa).
    public static final String NC_MAGIC_COOKIE = "hyncmagic";
    private static final Logger LOGGER = Logger.getLogger(TriggerNCWork.class.getName());

    private final ClusterControllerService ccs;
    private final String ncHost;
    private final int ncPort;
    private final String ncId;

    public TriggerNCWork(ClusterControllerService ccs, String ncHost, int ncPort, String ncId) {
        this.ccs = ccs;
        this.ncHost = ncHost;
        this.ncPort = ncPort;
        this.ncId = ncId;
    }
    @Override
    public final void run() {
        ccs.getExecutor().execute(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("Connecting NC service '" + ncId + "' at " + ncHost + ":" + ncPort);
                        }
                        Socket s = new Socket(ncHost, ncPort);
                        ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
                        oos.writeUTF(NC_MAGIC_COOKIE);
                        oos.writeUTF(serializeIni(ccs.getCCConfig().getIni()));
                        oos.close();
                        break;
                        // QQQ Should probably have an ACK here
                    } catch (IOException e) {
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.log(Level.WARNING, "Failed to contact NC service at " + ncHost +
                                    ":" + ncPort + "; will retry", e);
                        }
                    }
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
        });
    }

    /**
     * Utility routine to copy all keys from a named section in Ini a
     * to a named section in Ini b. We need to do this the hard way
     * because Ini4j reacts inscrutably when attempting to copy
     * Ini.Sections directly from one Ini to another.
     */
    private void copyIniSection(Ini a, String asect, Ini b, String bsect) {
        Ini.Section source = a.get(asect);
        for (String key : source.keySet()) {
            b.put(bsect, key, source.get(key));
        }
    }
    /**
     * Given an Ini object, serialize it to String with some enhancements.
     * @param ccini
     */
    String serializeIni(Ini ccini) throws IOException {
        Ini ini = new Ini();

        // First copy the global [nc] section to a new section named for
        // *this* NC, so that those values serve as defaults.
        String ncsection = "nc/" + ncId;
        copyIniSection(ccini, "nc", ini, ncsection);
        // Now copy all sections to their same name in the derived config.
        for (String section : ccini.keySet()) {
            copyIniSection(ccini, section, ini, section);
        }
        // Finally insert *this* NC's name into localnc section - this is a fixed
        // entry point so that NCs can determine where all their config is.
        ini.put("localnc", "id", ncId);
        StringWriter iniString = new StringWriter();
        ini.store(iniString);
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Returning Ini file:\n" + iniString.toString());
        }
        return iniString.toString();
    }
}
