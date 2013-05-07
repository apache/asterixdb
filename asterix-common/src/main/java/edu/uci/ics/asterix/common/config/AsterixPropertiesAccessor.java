package edu.uci.ics.asterix.common.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import edu.uci.ics.asterix.common.configuration.AsterixConfiguration;
import edu.uci.ics.asterix.common.configuration.Property;
import edu.uci.ics.asterix.common.configuration.Store;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class AsterixPropertiesAccessor {

    private final String metadataNodeName;
    private final Set<String> nodeNames;
    private final Map<String, String[]> stores;
    private final Map<String, String> asterixConfigurationParams;

    public AsterixPropertiesAccessor() throws AsterixException {
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
                throw new AsterixException("Could not find configuration file " + fileName);
            }
        }

        AsterixConfiguration asterixConfiguration = null;
        try {
            JAXBContext ctx = JAXBContext.newInstance(AsterixConfiguration.class);
            Unmarshaller unmarshaller = ctx.createUnmarshaller();
            asterixConfiguration = (AsterixConfiguration) unmarshaller.unmarshal(is);
        } catch (JAXBException e) {
            throw new AsterixException("Failed to read configuration file " + fileName);
        }
        metadataNodeName = asterixConfiguration.getMetadataNode();
        stores = new HashMap<String, String[]>();
        List<Store> configuredStores = asterixConfiguration.getStore();
        nodeNames = new HashSet<String>();
        for (Store store : configuredStores) {
            String trimmedStoreDirs = store.getStoreDirs().trim();
            stores.put(store.getNcId(), trimmedStoreDirs.split(","));
            nodeNames.add(store.getNcId());
        }
        asterixConfigurationParams = new HashMap<String, String>();
        for (Property p : asterixConfiguration.getProperty()) {
            asterixConfigurationParams.put(p.getName(), p.getValue());
        }
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

    public Set<String> getNodeNames() {
        return nodeNames;
    }

    public int getInt(String property, int defaultValue) {
        String propertyValue = asterixConfigurationParams.get(property);
        return propertyValue == null ? defaultValue : Integer.parseInt(propertyValue);
    }

    public String getString(String property, String defaultValue) {
        String propertyValue = asterixConfigurationParams.get(property);
        return propertyValue == null ? defaultValue : propertyValue;
    }
}
