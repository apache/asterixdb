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
package org.apache.hyracks.maven.license;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.ProjectBuildingException;

@Mojo(name = "licensedownload", requiresProject = true, requiresDependencyResolution = ResolutionScope.TEST, defaultPhase = LifecyclePhase.GENERATE_RESOURCES)
public class DownloadLicensesMojo extends LicenseMojo {

    @Parameter(required = true)
    private File downloadDir;

    @Parameter(defaultValue = "30")
    private int timeoutSecs;

    @java.lang.Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        try {
            init();
            addDependenciesToLicenseMap();
            final int timeoutMillis = (int) TimeUnit.SECONDS.toMillis(timeoutSecs);
            //noinspection ResultOfMethodCallIgnored
            downloadDir.mkdirs();
            AtomicInteger counter = new AtomicInteger();
            getLicenseMap().values().parallelStream().forEach(entry -> {
                final int i = counter.incrementAndGet();
                final String url = entry.getLicense().getUrl();
                String fileName = entry.getLicense().getContentFile(false);
                doDownload(timeoutMillis, i, url, fileName);
            });
        } catch (ProjectBuildingException e) {
            throw new MojoExecutionException("Unexpected exception: " + e, e);
        }
    }

    private void doDownload(int timeoutMillis, int id, String url, String fileName) {
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setConnectTimeout(timeoutMillis);
            conn.setReadTimeout(timeoutMillis);
            conn.setRequestMethod("GET");
            final File outFile = new File(downloadDir, fileName);
            getLog().info("[" + id + "] " + url + " -> " + outFile);
            final InputStream is = conn.getInputStream();
            try (FileOutputStream fos = new FileOutputStream(outFile);
                    Writer writer = new OutputStreamWriter(fos, StandardCharsets.UTF_8)) {
                IOUtils.copy(is, writer, conn.getContentEncoding());
            }
            getLog().info("[" + id + "] ...done!");
        } catch (IOException e) {
            getLog().warn("[" + id + "] ...error downloading " + url + ": " + e);
        }
    }
}
