/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.asterix.metadata.bootstrap;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

/**
 * Holder for Asterix properties values typically set as Java Properties.
 * Intended to live in the AsterixStateProxy so it can be accessed remotely.
 */
public class AsterixProperties implements Serializable {

    private static final long serialVersionUID = 1L;
    private static String metadataNodeName;
    private static Boolean isNewUniverse;
    private static HashSet<String> nodeNames;
    private static Map<String, String[]> stores;
    private static String outputDir;

    public static AsterixProperties INSTANCE = new AsterixProperties();

    private AsterixProperties() {
        try {
            initializeProperties();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private void initializeProperties() throws AlgebricksException {
        Properties p = new Properties();
        String fileName = System.getProperty(GlobalConfig.CONFIG_FILE_PROPERTY);
        if (fileName == null) {
            fileName = GlobalConfig.DEFAULT_CONFIG_FILE_NAME;
        }

        InputStream is = this.getClass().getClassLoader().getResourceAsStream(fileName);
        if (is == null) {
            try {
                fileName = GlobalConfig.DEFAULT_CONFIG_FILE_NAME;
                is = new FileInputStream(fileName);
            } catch (FileNotFoundException fnf) {
                throw new AlgebricksException("Could not find the configuration file " + fileName);
            }
        }
        try {
            p.load(is);
            is.close();
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
        Enumeration<String> pNames = (Enumeration<String>) p.propertyNames();
        stores = new HashMap<String, String[]>();
        boolean newUniverseChosen = false;
        String pn;
        String val;
        while (pNames.hasMoreElements()) {
            pn = pNames.nextElement();
            if (pn.equals("MetadataNode")) {
                val = p.getProperty(pn);
                metadataNodeName = val;
            } else if (pn.equals("NewUniverse")) {
                val = p.getProperty(pn);
                newUniverseChosen = true;
                isNewUniverse = Boolean.parseBoolean(val);
            } else if (pn.equals("OutputDir")) {
                val = p.getProperty(pn);
                outputDir = val;
            } else {
                String ncName = pn.substring(0, pn.indexOf('.'));
                val = p.getProperty(pn);
                String[] folderNames = val.split("\\s*,\\s*");
                stores.put(ncName, folderNames);
                nodeNames = new HashSet<String>();
                nodeNames.addAll(stores.keySet());
            }
        }
        if (metadataNodeName == null)
            throw new AlgebricksException("You need to specify the metadata node!");
        if (!newUniverseChosen)
            throw new AlgebricksException("You need to specify whether or not you want to start a new universe!");
    }

    public Boolean isNewUniverse() {
        return isNewUniverse;
    }

    public String getMetadataNodeName() {
        return metadataNodeName;
    }

    public String getMetadataStore() {
        return stores.get(metadataNodeName)[0];
    }

    public Map<String, String[]> getStores() {
        return stores;
    }

    public HashSet<String> getNodeNames() {
        return nodeNames;
    }

    public  String getOutputDir() {
        return outputDir;
    }
}
