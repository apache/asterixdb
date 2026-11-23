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
package org.apache.asterix.test.podman;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.model.Bind;

/**
 * Starts the published image with each way of setting its credentials. Run with
 * {@code mvn verify -Dit.test=PodmanEntrypointIT -pl asterixdb/asterix-podman} after building asterix-server.
 */
public class PodmanEntrypointIT {
    private static final String HOME = "/var/lib/asterix";
    private static final String HELPER = "/opt/apache-asterixdb/bin/asterixhelper";
    private static final String PASSWORD_FILE = "/tmp/password";
    private static final String PASSWORD = "-first pass word";
    private static final String OTHER_PASSWORD = "second-password";
    private static final Pattern GENERATED = Pattern.compile("Generated password for user admin: (\\S+)");
    private static final int OK = 200;
    private static final int UNAUTHORIZED = 401;

    private static DockerImageName image;

    @BeforeClass
    public static void buildImage() throws Exception {
        image = DockerImageName.parse(ServerImage.build("asterixdb/entrypoint-it:latest"));
    }

    @Test
    public void generatesPasswordOnlyOnFirstStart() throws Exception {
        try (GenericContainer<?> asterix = container()) {
            start(asterix);
            String password = generatedPassword(asterix);
            assertEquals(OK, query(asterix, "admin:" + password));
            assertEquals(UNAUTHORIZED, query(asterix, null));
            assertEquals("600", shell(asterix, "stat -c %a " + HOME + "/passwd"));

            DockerClientFactory.instance().client().restartContainerCmd(asterix.getContainerId()).exec();
            waitForCluster(asterix);
            assertEquals(OK, query(asterix, "admin:" + password));
            assertEquals(1, GENERATED.matcher(asterix.getLogs()).results().count());
        }
    }

    @Test
    public void usesConfiguredCredentialsWithoutExposingThem() throws Exception {
        try (GenericContainer<?> asterix =
                container().withEnv(Map.of("ASTERIX_USER", "dba", "ASTERIX_PASSWORD", PASSWORD))) {
            start(asterix);
            assertEquals(OK, query(asterix, "dba:" + PASSWORD));
            assertEquals(UNAUTHORIZED, query(asterix, "dba:" + OTHER_PASSWORD));
            assertEquals(UNAUTHORIZED, query(asterix, "admin:" + PASSWORD));
            assertFalse(asterix.getLogs().contains(PASSWORD));
            assertFalse(GENERATED.matcher(asterix.getLogs()).find());
            assertFalse("the server must not inherit the password",
                    shell(asterix, "cat /proc/1/environ").contains("ASTERIX_PASSWORD"));
        }
    }

    @Test
    public void readsPasswordFromFile() throws Exception {
        try (GenericContainer<?> asterix = container().withEnv("ASTERIX_PASSWORD_FILE", PASSWORD_FILE)
                .withCopyToContainer(Transferable.of(PASSWORD + "\n"), PASSWORD_FILE)) {
            start(asterix);
            assertEquals(OK, query(asterix, "admin:" + PASSWORD));
        }
    }

    @Test
    public void keepsCredentialsInVolumeUntilPasswordIsSet() throws Exception {
        String volume = "entrypoint-it-" + UUID.randomUUID();
        try {
            try (GenericContainer<?> asterix = withVolume(container(), volume).withEnv("ASTERIX_PASSWORD", PASSWORD)) {
                start(asterix);
                assertEquals(OK, query(asterix, "admin:" + PASSWORD));
            }
            try (GenericContainer<?> asterix = withVolume(container(), volume)) {
                start(asterix);
                assertEquals(OK, query(asterix, "admin:" + PASSWORD));
                assertFalse(GENERATED.matcher(asterix.getLogs()).find());
            }
            try (GenericContainer<?> asterix =
                    withVolume(container(), volume).withEnv("ASTERIX_PASSWORD", OTHER_PASSWORD)) {
                start(asterix);
                assertEquals(OK, query(asterix, "admin:" + OTHER_PASSWORD));
                assertEquals(UNAUTHORIZED, query(asterix, "admin:" + PASSWORD));
                assertEquals("1", shell(asterix, "wc -l < " + HOME + "/passwd"));
            }
        } finally {
            DockerClientFactory.instance().client().removeVolumeCmd(volume).exec();
        }
    }

    @Test
    public void disablesAuthentication() throws Exception {
        try (GenericContainer<?> asterix = container().withEnv("ASTERIX_AUTH", "none")) {
            start(asterix);
            assertEquals(OK, query(asterix, null));
            assertFalse(GENERATED.matcher(asterix.getLogs()).find());
        }
    }

    @Test
    public void refusesUnusableConfiguration() throws Exception {
        Map<List<String>, String> unusable = Map.of(
                List.of("ASTERIX_PASSWORD=" + PASSWORD, "ASTERIX_PASSWORD_FILE=" + PASSWORD_FILE), "cannot both be set",
                List.of("ASTERIX_PASSWORD_FILE=/does/not/exist"), "cannot read ASTERIX_PASSWORD_FILE",
                List.of("ASTERIX_PASSWORD_FILE=/dev/null"), "is empty", List.of("ASTERIX_USER=dba"),
                "ASTERIX_USER needs", List.of("ASTERIX_USER=d:ba", "ASTERIX_PASSWORD=" + PASSWORD),
                "ASTERIX_USER must not contain", List.of("ASTERIX_PASSWORD=pass:word"), "password must not contain",
                List.of("ASTERIX_AUTH=none", "ASTERIX_PASSWORD=" + PASSWORD), "cannot be combined",
                List.of("ASTERIX_AUTH=basic"), "ASTERIX_AUTH must be");
        try (GenericContainer<?> idle =
                container().withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("sleep", "infinity"))) {
            idle.start();
            for (Map.Entry<List<String>, String> entry : unusable.entrySet()) {
                List<String> command = new ArrayList<>(List.of("timeout", "60", "env"));
                command.addAll(entry.getKey());
                command.add("/entrypoint.sh");
                Container.ExecResult result = idle.execInContainer(command.toArray(new String[0]));
                assertEquals(entry.getKey() + ": " + result.getStderr(), 1, result.getExitCode());
                assertTrue(entry.getKey() + ": " + result.getStderr(), result.getStderr().contains(entry.getValue()));
            }
        }
    }

    private static GenericContainer<?> container() {
        return new GenericContainer<>(image);
    }

    private static GenericContainer<?> withVolume(GenericContainer<?> asterix, String volume) {
        return asterix
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withBinds(Bind.parse(volume + ":" + HOME)));
    }

    private static void start(GenericContainer<?> asterix) throws Exception {
        asterix.start();
        waitForCluster(asterix);
    }

    private static void waitForCluster(GenericContainer<?> asterix) throws Exception {
        Container.ExecResult result = asterix.execInContainer(HELPER, "wait_for_cluster", "-timeout", "120");
        assertEquals(asterix.getLogs(), 0, result.getExitCode());
    }

    private static String generatedPassword(GenericContainer<?> asterix) {
        Matcher matcher = GENERATED.matcher(asterix.getLogs());
        assertTrue(asterix.getLogs(), matcher.find());
        return matcher.group(1);
    }

    private static int query(GenericContainer<?> asterix, String credentials) throws Exception {
        String curl = "curl -s -o /dev/null -w %{http_code} http://localhost:19002/query/service"
                + " --data-urlencode 'statement=SELECT VALUE 1;'";
        Container.ExecResult result = credentials == null ? asterix.execInContainer("sh", "-c", curl)
                : asterix.execInContainer("sh", "-c", curl + " -u \"$0\"", credentials);
        assertEquals(result.getStderr(), 0, result.getExitCode());
        return Integer.parseInt(result.getStdout());
    }

    private static String shell(GenericContainer<?> asterix, String command) throws Exception {
        Container.ExecResult result = asterix.execInContainer("sh", "-c", command);
        assertEquals(result.getStderr(), 0, result.getExitCode());
        return result.getStdout().trim();
    }
}
