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
package org.apache.asterix.spidersilk;

import java.io.File;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.asterix.test.common.TestExecutor;

import me.arminb.spidersilk.SpiderSilkRunner;
import me.arminb.spidersilk.dsl.entities.Deployment;
import me.arminb.spidersilk.dsl.entities.PortType;
import me.arminb.spidersilk.dsl.entities.ServiceType;
import me.arminb.spidersilk.exceptions.RuntimeEngineException;

public class TestUtil {
    private static String mavenVersion;
    private static String asterixHome;

    public static Deployment getSimpleClusterDeployment() {
        mavenVersion = getMavenArtifactVersion();
        asterixHome = "/asterix/apache-asterixdb-" + mavenVersion;

        return new Deployment.DeploymentBuilder("simpleClusterDeployment")
                // Service Definitions
                .withService("asterix")
                .applicationPath("../asterix-server/target/asterix-server-" + mavenVersion + "-binary-assembly.zip",
                        "/asterix", false, true, false)
                .dockerFileAddress("docker/Dockerfile", false).dockerImage("spidersilk/test-asterix")
                .instrumentablePath(asterixHome + "/repo/asterix-server-" + mavenVersion + ".jar")
                .libraryPath(asterixHome + "/repo/*.jar").libraryPath(asterixHome + "/lib/*.jar")
                .logDirectory(asterixHome + "/logs").serviceType(ServiceType.JAVA).and()
                // Node Definitions
                .withNode("cc", "asterix").applicationPath("config", "/asterix/config")
                .startCommand(asterixHome + "/bin/asterixcc -config-file /asterix/config/cc.conf").tcpPort(19002).and()
                .withNode("nc1", "asterix").startCommand(asterixHome + "/bin/asterixncservice").and()
                .withNode("nc2", "asterix").startCommand(asterixHome + "/bin/asterixncservice").and().build();
    }

    public static String getMavenArtifactVersion() {
        Optional<String> version = Stream
                .of(Objects.requireNonNull(new File("../asterix-server/target")
                        .list((dir, name) -> name.matches("asterix-server-.*-binary-assembly.zip"))))
                .map(foo -> foo.replace("asterix-server-", "")).map(foo -> foo.replace("-binary-assembly.zip", ""))
                .findFirst();
        return version.orElseThrow(IllegalStateException::new);
    }

    public static void waitForClusterToBeUp(SpiderSilkRunner runner) throws RuntimeEngineException {
        runner.runtime().runCommandInNode("cc", asterixHome + "/bin/asterixhelper wait_for_cluster");
    }

    public static TestExecutor getTestExecutor(SpiderSilkRunner runner) {
        return new TestExecutor(runner.runtime().ip("cc"), runner.runtime().portMapping("cc", 19002, PortType.TCP));
    }
}
