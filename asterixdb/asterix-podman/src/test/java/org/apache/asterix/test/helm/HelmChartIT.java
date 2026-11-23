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
package org.apache.asterix.test.helm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.asterix.test.podman.ServerImage;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.k3s.K3sContainer;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.HostConfig;

/**
 * Deploys the packaged chart to a single-node k3s cluster, with an image built from the server assembly, and takes it
 * through install, restart, scale and uninstall. Run with {@code mvn verify -Phelm.tests -pl asterixdb/asterix-podman}
 * after building asterix-server. It has been run against rootful Docker and rootless Podman; the latter needs the
 * cpuset and io cgroup controllers delegated to the user (systemd {@code Delegate=} on {@code user@.service}). Where
 * {@code ~/.testcontainers.properties} points elsewhere, select the runtime with {@code TESTCONTAINERS_DOCKER_HOST}.
 */
public class HelmChartIT {
    private static final DockerImageName K3S_IMAGE = DockerImageName.parse("rancher/k3s:v1.34.13-k3s1");
    private static final String IMAGE_REPOSITORY = "docker.io/asterixdb/helm-it";
    private static final String IMAGE_TAG = "latest";
    private static final String IMAGE = IMAGE_REPOSITORY + ":" + IMAGE_TAG;
    private static final Path CHART_DIR = Path.of("target/helm/repo");
    private static final Path HELM = Path.of("target/helm/helm");
    private static final String IMAGE_ARCHIVE = "image.tar";
    private static final String IMAGE_IMPORT_DIR = "/helm-it-image";
    private static final String RELEASE = "smoke";
    private static final String CLIENT_POD = "client";
    private static final String API = "http://" + RELEASE + "-cc:19002";
    private static final int ROWS = 1000;
    private static final Pattern NODE_ID = Pattern.compile("\"node_id\"");
    private static final Duration HELM_TIMEOUT = Duration.ofMinutes(5);
    private static final Duration ROLLOUT_TIMEOUT = Duration.ofMinutes(5);
    private static final Duration CLUSTER_TIMEOUT = Duration.ofMinutes(5);
    private static final Duration UNINSTALL_TIMEOUT = Duration.ofMinutes(2);
    private static final Duration POLL_PERIOD = Duration.ofSeconds(2);

    private static K3sContainer k3s;
    private static Path kubeConfig;

    @Rule
    public final TestRule dumpClusterOnFailure = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            dumpCluster();
        }
    };

    @BeforeClass
    public static void startCluster() throws Exception {
        String image = ServerImage.build(IMAGE_REPOSITORY.substring("docker.io/".length()) + ":" + IMAGE_TAG);
        Path archiveDir = Files.createTempDirectory("helm-it-image");
        Path archive = archiveDir.resolve(IMAGE_ARCHIVE);
        try (InputStream saved = DockerClientFactory.instance().client().saveImageCmd(image).exec()) {
            Files.copy(saved, archive);
        }
        try {
            // containerd cannot use its overlay snapshotter when its directory is on the container's own overlay, and
            // kubelet would report disk pressure from the host's disk rather than from what this test uses. The image
            // has to arrive by bind mount, as files copied to a tmpfs mount land in the layer underneath it.
            k3s = new K3sContainer(K3S_IMAGE)
                    .withFileSystemBind(archiveDir.toString(), IMAGE_IMPORT_DIR, BindMode.READ_ONLY)
                    .withTmpFs(Map.of("/run", "", "/var/run", "", "/var/lib/rancher/k3s", "rw,exec,suid,dev",
                            "/var/lib/kubelet", "rw,exec,suid,dev"));
            if (isRootless()) {
                k3s.withCreateContainerCmdModifier(HelmChartIT::runInUserNamespace);
            }
            k3s.start();
            run("ctr", "-n", "k8s.io", "images", "import", IMAGE_IMPORT_DIR + "/" + IMAGE_ARCHIVE);
        } finally {
            Files.deleteIfExists(archive);
            Files.deleteIfExists(archiveDir);
        }
        kubeConfig = Files.createTempFile("helm-it-kubeconfig", ".yaml");
        Files.writeString(kubeConfig, k3s.getKubeConfigYaml());
        kubectl("run", CLIENT_POD, "--image=" + IMAGE, "--image-pull-policy=Never", "--restart=Never", "--command",
                "--", "sleep", "infinity");
        kubectl("wait", "--for=condition=Ready", "pod/" + CLIENT_POD, "--timeout=120s");
    }

    private static boolean isRootless() {
        List<String> securityOptions = DockerClientFactory.instance().client().infoCmd().exec().getSecurityOptions();
        return securityOptions != null && securityOptions.contains("name=rootless");
    }

    /**
     * Adapts k3s to a rootless runtime, where the container cannot write to the host's cgroup root that
     * {@link K3sContainer} binds in with a host cgroup namespace, and the kubelet runs in a user namespace.
     */
    private static void runInUserNamespace(CreateContainerCmd cmd) {
        HostConfig hostConfig = cmd.getHostConfig();
        hostConfig.withCgroupnsMode("private").withBinds(Arrays.stream(hostConfig.getBinds())
                .filter(bind -> !bind.getVolume().getPath().equals("/sys/fs/cgroup")).toArray(Bind[]::new));
        // cgroup v2 refuses to enable controllers on a cgroup that still holds processes, so k3s's own process
        // moves out of the namespace root before the kubelet tries to create kubepods under it, as k3d does.
        cmd.withEntrypoint("/bin/sh", "-c",
                "mkdir -p /sys/fs/cgroup/init"
                        + " && xargs -rn1 < /sys/fs/cgroup/cgroup.procs > /sys/fs/cgroup/init/cgroup.procs"
                        + " && sed -e 's/ / +/g' -e 's/^/+/' /sys/fs/cgroup/cgroup.controllers"
                        + " > /sys/fs/cgroup/cgroup.subtree_control && exec /bin/k3s \"$@\"",
                "k3s");
        List<String> command = new ArrayList<>(List.of(cmd.getCmd()));
        command.add("--kubelet-arg=feature-gates=KubeletInUserNamespace=true");
        cmd.withCmd(command);
    }

    @AfterClass
    public static void stopCluster() throws IOException {
        if (k3s != null) {
            k3s.stop();
        }
        if (kubeConfig != null) {
            Files.deleteIfExists(kubeConfig);
        }
    }

    @Test
    public void installRestartScaleUninstall() throws Exception {
        helm("install", RELEASE, packagedChart().toString(), "--set", "image.repository=" + IMAGE_REPOSITORY, "--set",
                "image.tag=" + IMAGE_TAG, "--set", "image.pullPolicy=Never", "--set", "numNodes=2", "--set",
                "cc.javaOpts=-Xmx1g", "--set", "nc.javaOpts=-Xmx1g", "--set", "rustfs.persistence.size=1Gi");
        awaitCluster(2);
        String password = decodedSecret("password");
        assertApiRequiresCredentials(password);
        loadData(password);
        assertRowsPresent(password);

        kubectl("rollout", "restart", "statefulset/" + RELEASE + "-cc", "statefulset/" + RELEASE + "-nc");
        awaitCluster(2);
        assertEquals(password, decodedSecret("password"));
        assertRowsPresent(password);

        scaleTo(3);
        assertRowsPresent(password);
        scaleTo(1);
        assertRowsPresent(password);

        helm("uninstall", RELEASE);
        awaitReleaseGone();
    }

    private static void scaleTo(int nodes) throws Exception {
        helm("upgrade", RELEASE, packagedChart().toString(), "--reuse-values", "--set", "numNodes=" + nodes);
        awaitCluster(nodes);
    }

    private static void assertApiRequiresCredentials(String password) throws Exception {
        assertEquals(401, query(null, "SELECT VALUE 1;").status);
        assertEquals(401, query("admin:" + password + "-wrong", "SELECT VALUE 1;").status);
        assertEquals(200, query("admin:" + password, "SELECT VALUE 1;").status);
    }

    private static void loadData(String password) throws Exception {
        String statements = "CREATE DATASET ds(id int) PRIMARY KEY id; UPSERT INTO ds (SELECT VALUE {\"id\": i} FROM "
                + "range(1, " + ROWS + ") i);";
        HttpResponse response = query("admin:" + password, statements);
        assertEquals(response.body, 200, response.status);
    }

    private static void assertRowsPresent(String password) throws Exception {
        HttpResponse response = query("admin:" + password, "SELECT VALUE COUNT(*) FROM ds;");
        assertEquals(response.body, 200, response.status);
        assertTrue(response.body, response.body.contains("\"results\": [ " + ROWS + " ]"));
    }

    private static HttpResponse query(String credentials, String statement) throws Exception {
        List<String> curl = new ArrayList<>(List.of("exec", CLIENT_POD, "--", "curl", "-sS", "-m", "120", "-w",
                "\n%{http_code}", API + "/query/service", "--data-urlencode", "statement=" + statement));
        if (credentials != null) {
            curl.addAll(List.of("-u", credentials));
        }
        return HttpResponse.parse(kubectl(curl.toArray(String[]::new)));
    }

    private static void awaitCluster(int nodes) throws Exception {
        for (String statefulSet : List.of("cc", "nc")) {
            kubectl("rollout", "status", "statefulset/" + RELEASE + "-" + statefulSet,
                    "--timeout=" + ROLLOUT_TIMEOUT.toSeconds() + "s");
        }
        String credentials = "admin:" + decodedSecret("password");
        Instant deadline = Instant.now().plus(CLUSTER_TIMEOUT);
        String cluster = "";
        while (Instant.now().isBefore(deadline)) {
            Container.ExecResult ping =
                    exec("kubectl", "exec", CLIENT_POD, "--", "curl", "-sS", "-m", "5", API + "/admin/ping");
            Container.ExecResult state = exec("kubectl", "exec", CLIENT_POD, "--", "curl", "-sS", "-m", "5", "-u",
                    credentials, API + "/admin/cluster");
            cluster = state.getStdout();
            if (ping.getStdout().contains("\"state\":\"ACTIVE\"") && countNodes(cluster) == nodes) {
                return;
            }
            Thread.sleep(POLL_PERIOD.toMillis());
        }
        throw new AssertionError("cluster did not become active with " + nodes + " nodes: " + cluster);
    }

    private static int countNodes(String cluster) {
        Matcher matcher = NODE_ID.matcher(cluster);
        int nodes = 0;
        while (matcher.find()) {
            nodes++;
        }
        return nodes;
    }

    private static void awaitReleaseGone() throws Exception {
        Instant deadline = Instant.now().plus(UNINSTALL_TIMEOUT);
        List<String> remaining = releaseResources();
        while (!remaining.isEmpty() && Instant.now().isBefore(deadline)) {
            Thread.sleep(POLL_PERIOD.toMillis());
            remaining = releaseResources();
        }
        assertEquals(List.of(), remaining);
    }

    private static List<String> releaseResources() throws Exception {
        String all =
                kubectl("get", "statefulset,deployment,pod,service,secret,configmap,pvc,networkpolicy", "-o", "name");
        List<String> remaining = new ArrayList<>();
        for (String resource : all.split("\n")) {
            if (resource.contains("/" + RELEASE + "-") || resource.contains(".release.v1." + RELEASE + ".")) {
                remaining.add(resource);
            }
        }
        return remaining;
    }

    private static String decodedSecret(String key) throws Exception {
        String encoded = kubectl("get", "secret", RELEASE + "-auth", "-o", "jsonpath={.data." + key + "}");
        return new String(Base64.getDecoder().decode(encoded.trim()), StandardCharsets.UTF_8);
    }

    private static Path packagedChart() throws IOException {
        List<Path> charts = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(CHART_DIR, "asterixdb-*.tgz")) {
            stream.forEach(charts::add);
        }
        assertEquals("expected exactly one packaged chart: " + charts, 1, charts.size());
        return charts.get(0);
    }

    private static String helm(String... args) throws Exception {
        List<String> command = new ArrayList<>(List.of(HELM.toString()));
        command.addAll(List.of(args));
        ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true);
        builder.environment().put("KUBECONFIG", kubeConfig.toString());
        Process process = builder.start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue("helm timed out", process.waitFor(HELM_TIMEOUT.toSeconds(), TimeUnit.SECONDS));
        assertEquals(String.join(" ", command) + ": " + output, 0, process.exitValue());
        return output;
    }

    private static String kubectl(String... args) throws Exception {
        List<String> command = new ArrayList<>(List.of("kubectl"));
        command.addAll(List.of(args));
        return run(command.toArray(String[]::new));
    }

    private static String run(String... command) throws Exception {
        Container.ExecResult result = exec(command);
        assertEquals(String.join(" ", command) + ": " + result.getStderr(), 0, result.getExitCode());
        return result.getStdout();
    }

    private static Container.ExecResult exec(String... command) throws Exception {
        return k3s.execInContainer(command);
    }

    private static void dumpCluster() {
        try {
            System.err.println(exec("kubectl", "get", "pods", "-A", "-o", "wide").getStdout());
            for (String pod : List.of(RELEASE + "-cc-0", RELEASE + "-nc-0")) {
                System.err.println(pod + ":\n" + exec("kubectl", "logs", pod, "-c", "log", "--tail=200").getStdout());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static final class HttpResponse {
        private final int status;
        private final String body;

        private HttpResponse(int status, String body) {
            this.status = status;
            this.body = body;
        }

        private static HttpResponse parse(String curlOutput) {
            int statusLine = curlOutput.lastIndexOf('\n');
            return new HttpResponse(Integer.parseInt(curlOutput.substring(statusLine + 1).trim()),
                    curlOutput.substring(0, statusLine));
        }
    }
}
