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
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.hyracks.maven.license.project.LicensedProjects;
import org.apache.hyracks.maven.license.project.Project;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.DefaultArtifact;
import org.apache.maven.artifact.metadata.ArtifactMetadata;
import org.apache.maven.artifact.repository.ArtifactRepository;
import org.apache.maven.artifact.repository.ArtifactRepositoryPolicy;
import org.apache.maven.artifact.repository.Authentication;
import org.apache.maven.artifact.repository.DefaultRepositoryRequest;
import org.apache.maven.artifact.repository.layout.ArtifactRepositoryLayout;
import org.apache.maven.artifact.repository.layout.DefaultRepositoryLayout;
import org.apache.maven.artifact.resolver.ArtifactResolutionRequest;
import org.apache.maven.artifact.resolver.ArtifactResolutionResult;
import org.apache.maven.project.MavenProject;
import org.apache.maven.project.ProjectBuildingException;
import org.apache.maven.repository.Proxy;

public class SourcePointerResolver {

    private final GenerateFileMojo mojo;

    private SourcePointerResolver(GenerateFileMojo mojo) {
        this.mojo = mojo;
    }

    public static void execute(GenerateFileMojo mojo) throws ProjectBuildingException, IOException {
        SourcePointerResolver instance = new SourcePointerResolver(mojo);
        instance.collectSourcePointers();
    }

    private ArtifactRepository getCentralRepository() {
        for (ArtifactRepository repo : mojo.getSession().getRequest().getRemoteRepositories()) {
            if ("central".equals(repo.getId())) {
                return repo;
            }
        }
        throw new IllegalStateException("Unable to find 'central' remote repository!");
    }

    private void collectSourcePointers() throws ProjectBuildingException, IOException {
        try (StubArtifactRepository stubRepo = new StubArtifactRepository()) {
            DefaultRepositoryRequest rr = new DefaultRepositoryRequest();
            rr.setLocalRepository(stubRepo);
            ArtifactRepository central = getCentralRepository();
            rr.setRemoteRepositories(Collections.singletonList(central));
            ArtifactResolutionRequest request = new ArtifactResolutionRequest(rr);
            for (LicensedProjects lp : mojo.getLicenseMap().values()) {
                if (lp.getLicense().getDisplayName() != null
                        && lp.getLicense().getDisplayName().toLowerCase().contains("cddl")) {
                    ensureCDDLSourcesPointer(lp.getProjects(), central, request);
                }
            }
        }
    }

    private void ensureCDDLSourcesPointer(Collection<Project> projects, ArtifactRepository central,
            ArtifactResolutionRequest request) throws ProjectBuildingException, IOException {
        for (Project p : projects) {
            if (p.getSourcePointer() != null) {
                continue;
            }
            mojo.getLog().debug("finding sources for artifact: " + p);
            Artifact sourcesArtifact = new DefaultArtifact(p.getGroupId(), p.getArtifactId(), p.getVersion(),
                    Artifact.SCOPE_COMPILE, "jar", "sources", null);
            MavenProject mavenProject = mojo.resolveDependency(sourcesArtifact);
            sourcesArtifact.setArtifactHandler(mavenProject.getArtifact().getArtifactHandler());
            final ArtifactRepository localRepo = mojo.getSession().getLocalRepository();
            final File marker = new File(localRepo.getBasedir(), localRepo.pathOf(sourcesArtifact) + ".oncentral");
            final File antimarker = new File(localRepo.getBasedir(), localRepo.pathOf(sourcesArtifact) + ".nocentral");
            boolean onCentral;
            if (marker.exists() || antimarker.exists()) {
                onCentral = marker.exists();
            } else {
                request.setArtifact(sourcesArtifact);
                ArtifactResolutionResult result = mojo.getArtifactResolver().resolve(request);
                mojo.getLog().debug("result: " + result);
                onCentral = result.isSuccess();
                if (onCentral) {
                    FileUtils.touch(marker);
                } else {
                    FileUtils.touch(antimarker);
                }
            }
            StringBuilder noticeBuilder = new StringBuilder("You may obtain ");
            noticeBuilder.append(p.getName()).append(" in Source Code form code here:\n");
            if (onCentral) {
                noticeBuilder.append(central.getUrl()).append("/").append(central.pathOf(sourcesArtifact));
            } else {
                mojo.getLog().warn("Unable to find sources in 'central' for " + p + ", falling back to project url: "
                        + p.getUrl());
                noticeBuilder.append(p.getUrl() != null ? p.getUrl() : "MISSING SOURCE POINTER");
            }
            p.setSourcePointer(noticeBuilder.toString());
        }
    }

    private static class StubArtifactRepository implements ArtifactRepository, AutoCloseable {
        private static final Random random = new Random();
        private final File tempDir;
        private final ArtifactRepositoryLayout layout;

        public StubArtifactRepository() {
            String tmpDir = System.getProperty("java.io.tmpdir", "/tmp");
            this.tempDir = new File(tmpDir, "repo" + random.nextInt());
            this.layout = new DefaultRepositoryLayout();
        }

        @java.lang.Override
        public ArtifactRepositoryLayout getLayout() {
            return layout;
        }

        @java.lang.Override
        public String pathOf(Artifact artifact) {
            return this.layout.pathOf(artifact);
        }

        @java.lang.Override
        public String getBasedir() {
            return tempDir.toString();
        }

        @java.lang.Override
        public void close() throws IOException {
            FileUtils.deleteDirectory(tempDir);

        }

        @java.lang.Override
        public String pathOfRemoteRepositoryMetadata(ArtifactMetadata artifactMetadata) {
            return null;
        }

        @java.lang.Override
        public String pathOfLocalRepositoryMetadata(ArtifactMetadata artifactMetadata,
                ArtifactRepository artifactRepository) {
            return null;
        }

        @java.lang.Override
        public String getUrl() {
            return null;
        }

        @java.lang.Override
        public void setUrl(String s) {
            // unused
        }

        @java.lang.Override
        public String getProtocol() {
            return null;
        }

        @java.lang.Override
        public String getId() {
            return "stub";
        }

        @java.lang.Override
        public void setId(String s) {
            // unused
        }

        @java.lang.Override
        public ArtifactRepositoryPolicy getSnapshots() {
            return null;
        }

        @java.lang.Override
        public void setSnapshotUpdatePolicy(ArtifactRepositoryPolicy artifactRepositoryPolicy) {
            // unused
        }

        @java.lang.Override
        public ArtifactRepositoryPolicy getReleases() {
            return null;
        }

        @java.lang.Override
        public void setReleaseUpdatePolicy(ArtifactRepositoryPolicy artifactRepositoryPolicy) {
            // unused
        }

        @java.lang.Override
        public void setLayout(ArtifactRepositoryLayout artifactRepositoryLayout) {
            // unused
        }

        @java.lang.Override
        public String getKey() {
            return null;
        }

        @java.lang.Override
        public boolean isUniqueVersion() {
            return false;
        }

        @java.lang.Override
        public boolean isBlacklisted() {
            return false;
        }

        @java.lang.Override
        public void setBlacklisted(boolean b) {
            // unused
        }

        @java.lang.Override
        public Artifact find(Artifact artifact) {
            return null;
        }

        @java.lang.Override
        public List<String> findVersions(Artifact artifact) {
            return Collections.emptyList();
        }

        @java.lang.Override
        public boolean isProjectAware() {
            return false;
        }

        @java.lang.Override
        public void setAuthentication(Authentication authentication) {
            // unused
        }

        @java.lang.Override
        public Authentication getAuthentication() {
            return null;
        }

        @java.lang.Override
        public void setProxy(Proxy proxy) {
            // unused
        }

        @java.lang.Override
        public Proxy getProxy() {
            return null;
        }

        @java.lang.Override
        public List<ArtifactRepository> getMirroredRepositories() {
            return Collections.emptyList();
        }

        @java.lang.Override
        public void setMirroredRepositories(List<ArtifactRepository> list) {
            // unused
        }
    }
}
