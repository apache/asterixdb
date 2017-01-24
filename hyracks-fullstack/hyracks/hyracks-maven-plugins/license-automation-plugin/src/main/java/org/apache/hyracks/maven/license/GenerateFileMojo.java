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
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.Override;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;
import freemarker.cache.FileTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hyracks.maven.license.freemarker.IndentDirective;
import org.apache.hyracks.maven.license.freemarker.LoadFileDirective;
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
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;
import org.apache.maven.project.ProjectBuildingException;
import org.apache.maven.repository.Proxy;

@Mojo(name = "generate",
        requiresProject = true,
        requiresDependencyResolution = ResolutionScope.TEST)
public class GenerateFileMojo extends LicenseMojo {

    public static final Pattern FOUNDATION_PATTERN = Pattern.compile("^\\s*This product includes software developed " +
                    "(at|by) The Apache Software Foundation \\(http://www.apache.org/\\).\\s*$".replace(" ", "\\s+"),
            Pattern.DOTALL | Pattern.MULTILINE);

    public static final Comparator<String> WHITESPACE_NORMALIZED_COMPARATOR =
            (o1, o2) -> o1.replaceAll("\\s+", " ").compareTo(o2.replaceAll("\\s+", " "));

    @Parameter(required = true)
    private File templateRootDir;

    @Parameter(defaultValue = "${project.build.directory}/generated-sources")
    private File outputDir;

    @Parameter
    private List<GeneratedFile> generatedFiles = new ArrayList<>();

    @Parameter(defaultValue = "${project.build.sourceEncoding}")
    private String encoding;

    @Parameter
    private File licenseMapOutputFile;

    @Parameter
    private List<ExtraLicenseFile> extraLicenseMaps = new ArrayList<>();

    @Parameter
    protected Map<String, String> templateProperties = new HashMap<>();

    @Parameter
    private boolean stripFoundationAssertionFromNotices = true;

    private SortedMap<String, SortedSet<Project>> noticeMap;

    @java.lang.Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        try {
            init();
            readExtraMaps();
            addDependenciesToLicenseMap();
            resolveLicenseContent();
            resolveNoticeFiles();
            resolveLicenseFiles();
            rebuildLicenseContentProjectMap();
            combineCommonGavs();
            collectSourceAssemblies();
            persistLicenseMap();
            buildNoticeProjectMap();
            generateFiles();
        } catch (IOException | TemplateException | ProjectBuildingException e) {
            throw new MojoExecutionException("Unexpected exception: " + e, e);
        }
    }

    private void collectSourceAssemblies() throws ProjectBuildingException, IOException {
        try (StubArtifactRepository stubRepo = new StubArtifactRepository()) {
            DefaultRepositoryRequest rr = new DefaultRepositoryRequest();
            rr.setLocalRepository(stubRepo);
            ArtifactRepository central = getCentralRepository();
            rr.setRemoteRepositories(Collections.singletonList(central));
            ArtifactResolutionRequest request = new ArtifactResolutionRequest(rr);
            for (LicensedProjects lp : licenseMap.values()) {
                if (lp.getLicense().getDisplayName() != null
                        && lp.getLicense().getDisplayName().toLowerCase().contains("cddl")) {
                    ensureCDDLSourcesPointer(lp.getProjects(), central, request);
                }
            }
        }
    }

    private void ensureCDDLSourcesPointer(Collection<Project> projects, ArtifactRepository central,
                                          ArtifactResolutionRequest request) throws ProjectBuildingException {
        for (Project p : projects) {
            if (p.getSourcePointer() != null) {
                continue;
            }
            getLog().debug("finding sources for artifact: " + p);
            Artifact sourcesArtifact = new DefaultArtifact(p.getGroupId(), p.getArtifactId(),
                    p.getVersion(), Artifact.SCOPE_COMPILE, "jar", "sources", null);
            MavenProject mavenProject = resolveDependency(sourcesArtifact);
            sourcesArtifact.setArtifactHandler(mavenProject.getArtifact().getArtifactHandler());

            request.setArtifact(sourcesArtifact);
            ArtifactResolutionResult result = artifactResolver.resolve(request);
            getLog().debug("result: " + result);
            StringBuilder noticeBuilder = new StringBuilder("You may obtain ");
            noticeBuilder.append(p.getName()).append(" in Source Code form code here:\n");
            if (result.isSuccess()) {
                noticeBuilder.append(central.getUrl()).append("/").append(central.pathOf(sourcesArtifact));
            } else {
                getLog().warn("Unable to find sources in 'central' for " + p + ", falling back to project url: "
                        + p.getUrl());
                noticeBuilder.append(p.getUrl() != null ? p.getUrl() : "MISSING SOURCE POINTER");
            }
            p.setSourcePointer(noticeBuilder.toString());
        }
    }

    private ArtifactRepository getCentralRepository() {
        for (ArtifactRepository repo : session.getRequest().getRemoteRepositories()) {
            if ("central".equals(repo.getId())) {
                return repo;
            }
        }
        throw new IllegalStateException("Unable to find 'central' remote repository!");
    }

    private void resolveLicenseContent() throws IOException {
        Set<LicenseSpec> licenseSpecs = new HashSet<>();
        for (LicensedProjects licensedProjects : licenseMap.values()) {
            licenseSpecs.add(licensedProjects.getLicense());
        }
        licenseSpecs.addAll(urlToLicenseMap.values());
        for (LicenseSpec license : licenseSpecs) {
            resolveLicenseContent(license, true);
        }
    }

    private String resolveLicenseContent(LicenseSpec license, boolean bestEffort) throws IOException {
        if (license.getContent() == null) {
            getLog().debug("Resolving content for " + license.getUrl() + " (" + license.getContentFile() + ")");
            File cFile = new File(license.getContentFile());
            if (!cFile.isAbsolute()) {
                cFile = new File(licenseDirectory, license.getContentFile());
            }
            if (!cFile.exists()) {
                if (!bestEffort) {
                    getLog().warn("MISSING: license content file (" + cFile + ") for url: " + license.getUrl());
                    license.setContent("MISSING: " + license.getContentFile() + " (" + license.getUrl() + ")");
                }
            } else {
                getLog().info("Reading license content from file: " + cFile);
                StringWriter sw = new StringWriter();
                LicenseUtil.readAndTrim(sw, cFile);
                license.setContent(sw.toString());
            }
        }
        return license.getContent();
    }

    private void combineCommonGavs() {
        for (LicensedProjects licensedProjects : licenseMap.values()) {
            Map<String, Project> projectMap = new HashMap<>();
            for (Iterator<Project> iter = licensedProjects.getProjects().iterator(); iter.hasNext(); ) {
                Project project = iter.next();
                if (projectMap.containsKey(project.gav())) {
                    Project first = projectMap.get(project.gav());
                    first.setLocation(first.getLocation() + "," + project.getLocation());
                    iter.remove();
                } else {
                    projectMap.put(project.gav(), project);
                }
            }
        }
    }

    private void generateFiles() throws TemplateException, IOException {
        Map<String, Object> props = getProperties();

        Configuration config = new Configuration();
        config.setTemplateLoader(new FileTemplateLoader(templateRootDir));
        for (GeneratedFile generation : generatedFiles) {
            Template template = config.getTemplate(generation.getTemplate());

            if (template == null) {
                throw new IOException("Could not load template " + generation.getTemplate());
            }

            outputDir.mkdirs();
            final File file = new File(outputDir, generation.getOutputFile());
            getLog().info("Writing " + file + "...");
            try (final FileWriter writer = new FileWriter(file)) {
                template.process(props, writer);
            }
        }
    }

    protected Map<String, Object> getProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put("indent", new IndentDirective());
        props.put("loadfile", new LoadFileDirective());
        props.put("project", project);
        props.put("noticeMap", noticeMap.entrySet());
        props.put("licenseMap", licenseMap.entrySet());
        props.put("licenses", urlToLicenseMap.values());
        props.putAll(templateProperties);
        return props;
    }

    private void readExtraMaps() throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper();
        for (ExtraLicenseFile extraLicenseFile : extraLicenseMaps) {
            for (LicensedProjects projects :
                    objectMapper.readValue(extraLicenseFile.getFile(), LicensedProjects[].class)) {
                LicenseSpec spec = urlToLicenseMap.get(projects.getLicense().getUrl());
                if (spec != null) {
                    // TODO(mblow): probably we should always favor the extra map...
                    // propagate any license content we may have with what already has been loaded
                    if (projects.getLicense().getContent() != null &&
                            spec.getContent() == null) {
                        spec.setContent(projects.getLicense().getContent());
                    }
                    // propagate any license displayName we may have with what already has been loaded
                    if (projects.getLicense().getDisplayName() != null &&
                            spec.getDisplayName() == null) {
                        spec.setDisplayName(projects.getLicense().getDisplayName());
                    }
                }
                for (Project p : projects.getProjects()) {
                    p.setLocation(extraLicenseFile.getLocation());
                    addProject(p, projects.getLicense(), extraLicenseFile.isAdditive());
                }
            }
        }
    }

    private void persistLicenseMap() throws IOException {
        if (licenseMapOutputFile != null) {
            licenseMapOutputFile.getParentFile().mkdirs();
            SequenceWriter sw = new ObjectMapper().writerWithDefaultPrettyPrinter()
                    .writeValues(licenseMapOutputFile).init(true);
            for (LicensedProjects entry : licenseMap.values()) {
                sw.write(entry);
            }
            sw.close();
        }
    }

    private void rebuildLicenseContentProjectMap() throws IOException {
        int counter = 0;
        Map<String, LicensedProjects> licenseMap2 = new TreeMap<>(WHITESPACE_NORMALIZED_COMPARATOR);
        for (LicensedProjects lps : licenseMap.values()) {
            for (Project p : lps.getProjects()) {
                String licenseText = p.getLicenseText();
                if (licenseText == null) {
                    getLog().warn("Using license other than from within artifact: " + p.gav());
                    licenseText = resolveLicenseContent(lps.getLicense(), false);
                }
                LicenseSpec spec = lps.getLicense();
                if (spec.getDisplayName() == null) {
                    LicenseSpec canonicalLicense = urlToLicenseMap.get(spec.getUrl());
                    if (canonicalLicense != null) {
                        spec.setDisplayName(canonicalLicense.getDisplayName());
                    }
                }
                if (!licenseMap2.containsKey(licenseText)) {
                    if (!licenseText.equals(lps.getLicense().getContent())) {
                        spec = new LicenseSpec(new ArrayList<>(), licenseText, null, spec.getDisplayName(),
                                spec.getMetric(), spec.getUrl() + (counter++));
                    }
                    licenseMap2.put(licenseText, new LicensedProjects(spec));
                }
                final LicensedProjects lp2 = licenseMap2.get(licenseText);
                if (lp2.getLicense().getDisplayName() == null) {
                    lp2.getLicense().setDisplayName(lps.getLicense().getDisplayName());
                }
                lp2.addProject(p);
            }
        }
        licenseMap = licenseMap2;
    }

    private Set<Project> getProjects() {
        Set<Project> projects = new HashSet<>();
        licenseMap.values().forEach(p -> projects.addAll(p.getProjects()));
        return projects;
    }

    private void buildNoticeProjectMap() {
        noticeMap = new TreeMap<>(WHITESPACE_NORMALIZED_COMPARATOR);
        for (Project p : getProjects()) {
            prependSourcePointerToNotice(p);
            final String noticeText = p.getNoticeText();
            if (noticeText == null) {
                continue;
            }
            if (!noticeMap.containsKey(noticeText)) {
                noticeMap.put(noticeText, new TreeSet<>(Project.PROJECT_COMPARATOR));
            }
            noticeMap.get(noticeText).add(p);
        }
    }

    private void prependSourcePointerToNotice(Project project) {
        if (project.getSourcePointer() != null) {
            String notice = project.getSourcePointer().replace("\n", "\n    ");
            if (project.getNoticeText() != null) {
                notice += "\n\n" + project.getNoticeText();
            }
            project.setNoticeText(notice);
        }
    }

    private void resolveNoticeFiles() throws MojoExecutionException, IOException {
        resolveArtifactFiles("NOTICE", entry -> entry.getName().matches("(.*/|^)" + "NOTICE" + "(.txt)?"),
                Project::setNoticeText,
                text -> stripFoundationAssertionFromNotices ? FOUNDATION_PATTERN.matcher(text).replaceAll("") : text);
    }

    private void resolveLicenseFiles() throws MojoExecutionException, IOException {
        resolveArtifactFiles("LICENSE", entry -> entry.getName().matches("(.*/|^)" + "LICENSE" + "(.txt)?"),
                Project::setLicenseText, UnaryOperator.identity());
    }

    private void resolveArtifactFiles(final String name, Predicate<JarEntry> filter,
                                      BiConsumer<Project, String> consumer, UnaryOperator<String> contentTransformer)
            throws MojoExecutionException, IOException {
        for (Project p : getProjects()) {
            File artifactFile = new File(p.getArtifactPath());
            if (!artifactFile.exists()) {
                throw new MojoExecutionException("Artifact file " + artifactFile + " does not exist!");
            } else if (!artifactFile.getName().endsWith(".jar")) {
                getLog().info("Skipping unknown artifact file type: " + artifactFile);
                continue;
            }
            try (JarFile jarFile = new JarFile(artifactFile)) {
                SortedMap<String, JarEntry> matches = gatherMatchingEntries(jarFile,
                        filter);
                if (matches.isEmpty()) {
                    getLog().warn("No " + name + " file found for " + p.gav());
                } else {
                    if (matches.size() > 1) {
                        getLog().warn("Multiple " + name + " files found for " + p.gav() + ": " + matches.keySet()
                                + "; taking first");
                    } else {
                        getLog().info(p.gav() + " has " + name + " file: " + matches.keySet());
                    }
                    resolveContent(p, jarFile, matches.values().iterator().next(),
                            contentTransformer, consumer, name);
                }
            }
        }
    }

    private void resolveContent(Project project, JarFile jarFile, JarEntry entry, UnaryOperator<String> transformer,
                                BiConsumer<Project, String> contentConsumer, final String name) throws IOException {
        String text = IOUtils.toString(jarFile.getInputStream(entry), StandardCharsets.UTF_8);
        text = transformer.apply(text);
        text = LicenseUtil.trim(text);
        if (text.length() == 0) {
            getLog().warn("Ignoring empty " + name + " file ( " + entry + ") for " + project.gav());
        } else {
            contentConsumer.accept(project, text);
            getLog().debug("Resolved " + name + " text for " + project.gav() + ": \n" + text);
        }
    }

    private SortedMap<String, JarEntry> gatherMatchingEntries(JarFile jarFile, Predicate<JarEntry> filter) {
        SortedMap<String, JarEntry> matches = new TreeMap<>();
        Enumeration<JarEntry> entries = jarFile.entries();
        while (entries.hasMoreElements()) {
            JarEntry entry = entries.nextElement();
            if (filter.test(entry)) {
                matches.put(entry.getName(), entry);
            }
        }
        return matches;
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

        @Override
        public ArtifactRepositoryLayout getLayout() {
            return layout;
        }

        @Override
        public String pathOf(Artifact artifact) {
            return this.layout.pathOf(artifact);
        }

        @Override
        public String getBasedir() {
            return tempDir.toString();
        }

        @Override
        public void close() throws IOException {
            FileUtils.deleteDirectory(tempDir);

        }

        @Override
        public String pathOfRemoteRepositoryMetadata(ArtifactMetadata artifactMetadata) {
            return null;
        }

        @Override
        public String pathOfLocalRepositoryMetadata(ArtifactMetadata artifactMetadata,
                                                    ArtifactRepository artifactRepository) {
            return null;
        }

        @Override
        public String getUrl() {
            return null;
        }

        @Override
        public void setUrl(String s) {
            // unused
        }

        @Override
        public String getProtocol() {
            return null;
        }

        @Override
        public String getId() {
            return "stub";
        }

        @Override
        public void setId(String s) {
            // unused
        }

        @Override
        public ArtifactRepositoryPolicy getSnapshots() {
            return null;
        }

        @Override
        public void setSnapshotUpdatePolicy(ArtifactRepositoryPolicy artifactRepositoryPolicy) {
            // unused
        }

        @Override
        public ArtifactRepositoryPolicy getReleases() {
            return null;
        }

        @Override
        public void setReleaseUpdatePolicy(ArtifactRepositoryPolicy artifactRepositoryPolicy) {
            // unused
        }

        @Override
        public void setLayout(ArtifactRepositoryLayout artifactRepositoryLayout) {
            // unused
        }

        @Override
        public String getKey() {
            return null;
        }

        @Override
        public boolean isUniqueVersion() {
            return false;
        }

        @Override
        public boolean isBlacklisted() {
            return false;
        }

        @Override
        public void setBlacklisted(boolean b) {
            // unused
        }

        @Override
        public Artifact find(Artifact artifact) {
            return null;
        }

        @Override
        public List<String> findVersions(Artifact artifact) {
            return Collections.emptyList();
        }

        @Override
        public boolean isProjectAware() {
            return false;
        }

        @Override
        public void setAuthentication(Authentication authentication) {
            // unused
        }

        @Override
        public Authentication getAuthentication() {
            return null;
        }

        @Override
        public void setProxy(Proxy proxy) {
            // unused
        }

        @Override
        public Proxy getProxy() {
            return null;
        }
    }
}

