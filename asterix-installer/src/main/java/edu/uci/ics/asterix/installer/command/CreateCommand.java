/*
 * Copyright 2009-2012 by The Regents of the University of California
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
package edu.uci.ics.asterix.installer.command;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.kohsuke.args4j.Option;

import edu.uci.ics.asterix.event.schema.cluster.Cluster;
import edu.uci.ics.asterix.event.schema.cluster.Env;
import edu.uci.ics.asterix.event.schema.cluster.Property;
import edu.uci.ics.asterix.event.schema.pattern.Patterns;
import edu.uci.ics.asterix.installer.driver.ManagixDriver;
import edu.uci.ics.asterix.installer.driver.ManagixUtil;
import edu.uci.ics.asterix.installer.error.VerificationUtil;
import edu.uci.ics.asterix.installer.events.PatternCreator;
import edu.uci.ics.asterix.installer.model.AsterixInstance;
import edu.uci.ics.asterix.installer.model.AsterixRuntimeState;
import edu.uci.ics.asterix.installer.service.ServiceProvider;

public class CreateCommand extends AbstractCommand {

    private String asterixInstanceName;
    private Cluster cluster;

    @Override
    protected void execCommand() throws Exception {
        asterixInstanceName = ((CreateConfig) config).name;
        ManagixUtil.validateAsterixInstanceNotExists(asterixInstanceName);
        CreateConfig createConfig = (CreateConfig) config;
        JAXBContext ctx = JAXBContext.newInstance(Cluster.class);
        Unmarshaller unmarshaller = ctx.createUnmarshaller();
        cluster = (Cluster) unmarshaller.unmarshal(new File(createConfig.clusterPath));
        AsterixInstance asterixInstance = ManagixUtil.createAsterixInstance(asterixInstanceName, cluster,
                ((CreateConfig) config).asterixConf);
        ManagixUtil.createAsterixZip(asterixInstance, true);
        List<Property> clusterProperties = new ArrayList<Property>();
        clusterProperties.add(new Property("HYRACKS_HOME", cluster.getWorkingDir().getDir() + File.separator
                + "hyracks"));
        clusterProperties.add(new Property("JAVA_OPTS", "-Xmx" + cluster.getRam()));
        clusterProperties.add(new Property("CLUSTER_NET_IP", cluster.getMasterNode().getClusterIp()));
        clusterProperties.add(new Property("CLIENT_NET_IP", cluster.getMasterNode().getIp()));
        clusterProperties.add(new Property("LOG_DIR", cluster.getLogdir()));
        clusterProperties.add(new Property("JAVA_HOME", cluster.getJavaHome()));
        cluster.setEnv(new Env(clusterProperties));

        PatternCreator pc = new PatternCreator();
        Patterns patterns = pc.getStartAsterixPattern(asterixInstanceName, cluster);
        ManagixUtil.getEventrixClient(cluster).submit(patterns);

        AsterixRuntimeState runtimeState = VerificationUtil.getAsterixRuntimeState(asterixInstance);
        VerificationUtil.updateInstanceWithRuntimeDescription(asterixInstance, runtimeState, true);
        ServiceProvider.INSTANCE.getLookupService().writeAsterixInstance(asterixInstance);
        System.out.println(asterixInstance.getDescription(false));
        ManagixUtil.deleteDirectory(ManagixDriver.getManagixHome() + File.separator + ManagixDriver.ASTERIX_DIR
                + File.separator + asterixInstanceName);

    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new CreateConfig();
    }

    public Cluster getCluster() {
        return cluster;
    }

    public String getAsterixInstanceName() {
        return asterixInstanceName;
    }

    @Override
    protected String getUsageDescription() {
        // TODO Auto-generated method stub
        return null;
    }
}

class CreateConfig implements CommandConfig {

    @Option(name = "-h", required = false, usage = "Help")
    public boolean help = false;

    @Option(name = "-n", required = true, usage = "Name of Asterix Instance")
    public String name;

    @Option(name = "-c", required = true, usage = "Path to cluster configuration")
    public String clusterPath;

    @Option(name = "-a", required = true, usage = "Path to cluster configuration")
    public String asterixConf;

}
