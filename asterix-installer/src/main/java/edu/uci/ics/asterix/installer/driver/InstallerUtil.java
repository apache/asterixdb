package edu.uci.ics.asterix.installer.driver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import edu.uci.ics.asterix.common.configuration.AsterixConfiguration;
import edu.uci.ics.asterix.event.error.OutputHandler;
import edu.uci.ics.asterix.event.management.EventrixClient;
import edu.uci.ics.asterix.event.schema.cluster.Cluster;
import edu.uci.ics.asterix.event.service.AsterixEventService;

public class InstallerUtil {

    private static final String DEFAULT_ASTERIX_CONFIGURATION_PATH = "conf" + File.separator + "asterix-configuration.xml";

    public static AsterixConfiguration getAsterixConfiguration(String asterixConf) throws FileNotFoundException,
            IOException, JAXBException {
        if (asterixConf == null) {
            asterixConf = InstallerDriver.getManagixHome() + File.separator + DEFAULT_ASTERIX_CONFIGURATION_PATH;
        }
        File file = new File(asterixConf);
        JAXBContext ctx = JAXBContext.newInstance(AsterixConfiguration.class);
        Unmarshaller unmarshaller = ctx.createUnmarshaller();
        AsterixConfiguration asterixConfiguration = (AsterixConfiguration) unmarshaller.unmarshal(file);
        return asterixConfiguration;
    }

    public static EventrixClient getEventrixClient(Cluster cluster) throws Exception {
        return new EventrixClient(AsterixEventService.getEventHome(), cluster, false,
                OutputHandler.INSTANCE);
    }

}
