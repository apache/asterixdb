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

import static org.apache.hyracks.maven.license.GenerateFileMojo.EmbeddedArtifact.LICENSE;
import static org.apache.hyracks.maven.license.GenerateFileMojo.EmbeddedArtifact.NOTICE;
import static org.apache.hyracks.maven.license.LicenseUtil.toGav;
import static org.apache.hyracks.maven.license.ProjectFlag.ALTERNATE_LICENSE_FILE;
import static org.apache.hyracks.maven.license.ProjectFlag.ALTERNATE_NOTICE_FILE;
import static org.apache.hyracks.maven.license.ProjectFlag.IGNORE_MISSING_EMBEDDED_LICENSE;
import static org.apache.hyracks.maven.license.ProjectFlag.IGNORE_MISSING_EMBEDDED_NOTICE;
import static org.apache.hyracks.maven.license.ProjectFlag.IGNORE_NOTICE_OVERRIDE;
import static org.apache.hyracks.maven.license.ProjectFlag.IGNORE_SHADOWED_DEPENDENCIES;
import static org.apache.hyracks.maven.license.ProjectFlag.ON_MULTIPLE_EMBEDDED_LICENSE;
import static org.apache.hyracks.maven.license.ProjectFlag.ON_MULTIPLE_EMBEDDED_NOTICE;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.maven.license.freemarker.IndentDirective;
import org.apache.hyracks.maven.license.freemarker.LoadFileDirective;
import org.apache.hyracks.maven.license.project.LicensedProjects;
import org.apache.hyracks.maven.license.project.Project;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.DefaultArtifact;
import org.apache.maven.artifact.handler.ArtifactHandler;
import org.apache.maven.artifact.handler.DefaultArtifactHandler;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;
import org.apache.maven.project.ProjectBuildingException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;

import freemarker.cache.FileTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

@Mojo(name = "generate", requiresProject = true, requiresDependencyResolution = ResolutionScope.TEST)
public class GenerateFileMojo extends LicenseMojo {

    public static final Pattern FOUNDATION_PATTERN = Pattern.compile(
            "^\\s*This product includes software developed "
                    + "(at|by) The Apache Software Foundation \\(http://www.apache.org/\\).\\s*$".replace(" ", "\\s+"),
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
    private boolean stripFoundationAssertionFromNotices = false;

    @Parameter
    private boolean includeShadowedDependencies = true;

    @Parameter
    private boolean validateShadowLicenses = false;

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
            SourcePointerResolver.execute(this);
            buildNoticeProjectMap();
            persistLicenseMap();
            generateFiles();
            if (seenWarning && failOnWarning) {
                throw new MojoFailureException(
                        "'failOnWarning' enabled and warning(s) (or error(s)) occurred during execution; see output");
            }
        } catch (IOException | TemplateException | ProjectBuildingException e) {
            throw new MojoExecutionException("Unexpected exception: " + e, e);
        }
    }

    private void resolveLicenseContent() throws IOException {
        Set<LicenseSpec> licenseSpecs = new HashSet<>();
        for (LicensedProjects licensedProjects : licenseMap.values()) {
            licenseSpecs.add(licensedProjects.getLicense());
        }
        licenseSpecs.addAll(urlToLicenseMap.values());
        for (LicenseSpec license : licenseSpecs) {
            resolveArtifactContent(license, true, false);
        }
    }

    private String resolveArtifactContent(ArtifactSpec artifact, boolean bestEffort, boolean suppressWarning)
            throws IOException {
        if (artifact.getContent() == null) {
            getLog().debug("Resolving content for " + artifact.getUrl() + " (" + artifact.getContentFile() + ")");
            File cFile = new File(artifact.getContentFile());
            if (!cFile.isAbsolute()) {
                for (File directory : licenseDirectories) {
                    cFile = new File(directory, artifact.getContentFile());
                    if (cFile.exists()) {
                        break;
                    }
                }
            }
            if (!cFile.exists()) {
                if (!bestEffort) {
                    if (!suppressWarning) {
                        getLog().warn("MISSING: content file (" + cFile + ") for url: " + artifact.getUrl());
                    }
                    artifact.setContent("MISSING: " + artifact.getContentFile() + " (" + artifact.getUrl() + ")");
                }
            } else {
                getLog().info("Reading content from file: " + cFile);
                StringWriter sw = new StringWriter();
                LicenseUtil.readAndTrim(sw, cFile);
                artifact.setContent(sw.toString());
            }
        }
        return artifact.getContent();
    }

    private void combineCommonGavs() {
        for (LicensedProjects licensedProjects : licenseMap.values()) {
            Map<String, Project> projectMap = new HashMap<>();
            for (Iterator<Project> iter = licensedProjects.getProjects().iterator(); iter.hasNext();) {
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
            Template template = config.getTemplate(generation.getTemplate(), StandardCharsets.UTF_8.name());

            if (template == null) {
                throw new IOException("Could not load template " + generation.getTemplate());
            }

            final File file = new File(outputDir, generation.getOutputFile());
            file.getParentFile().mkdirs();
            getLog().info("Writing " + file + "...");
            try (final FileOutputStream fos = new FileOutputStream(file);
                    final Writer writer = new OutputStreamWriter(fos, StandardCharsets.UTF_8)) {
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
            for (LicensedProjects projects : objectMapper.readValue(extraLicenseFile.getFile(),
                    LicensedProjects[].class)) {
                LicenseSpec spec = urlToLicenseMap.get(projects.getLicense().getUrl());
                if (spec != null) {
                    // TODO(mblow): probably we should always favor the extra map...
                    // propagate any license content we may have with what already has been loaded
                    if (projects.getLicense().getContent() != null && spec.getContent() == null) {
                        spec.setContent(projects.getLicense().getContent());
                    }
                    // propagate any license displayName we may have with what already has been loaded
                    if (projects.getLicense().getDisplayName() != null && spec.getDisplayName() == null) {
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
            SequenceWriter sw =
                    new ObjectMapper().writerWithDefaultPrettyPrinter().writeValues(licenseMapOutputFile).init(true);
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
                    if (validateProjectLicense(p)) {
                        warnUnlessFlag(p.gav(), IGNORE_MISSING_EMBEDDED_LICENSE,
                                "Using license other than from within artifact: " + p.gav() + " (" + lps.getLicense()
                                        + ")");
                    }
                    licenseText = resolveArtifactContent(lps.getLicense(), false, !validateProjectLicense(p));
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

    private boolean validateProjectLicense(Project p) {
        return !p.isShadowed() || validateShadowLicenses;
    }

    private Set<Project> getProjects() {
        Set<Project> projects = new HashSet<>();
        licenseMap.values().forEach(p -> projects.addAll(p.getProjects()));
        return projects;
    }

    private void buildNoticeProjectMap() throws IOException {
        noticeMap = new TreeMap<>(WHITESPACE_NORMALIZED_COMPARATOR);
        for (Project p : getProjects()) {
            String noticeText = p.getNoticeText();
            if (noticeText == null && noticeOverrides.containsKey(p.gav())) {
                String noticeUrl = noticeOverrides.get(p.gav());
                if (validateProjectLicense(p)) {
                    warnUnlessFlag(p.gav(), IGNORE_NOTICE_OVERRIDE,
                            "Using notice other than from within artifact: " + p.gav() + " (" + noticeUrl + ")");
                }
                p.setNoticeText(resolveArtifactContent(new NoticeSpec(noticeUrl), false, p.isShadowed()));
            } else if (noticeText == null && !noticeOverrides.containsKey(p.gav())
                    && Boolean.TRUE.equals(getProjectFlag(p.gav(), IGNORE_NOTICE_OVERRIDE))) {
                getLog().warn(p + " has IGNORE_NOTICE_OVERRIDE flag set, but no override defined...");
            }
            prependSourcePointerToNotice(p);
            noticeText = p.getNoticeText();
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

    enum EmbeddedArtifact {
        NOTICE,
        LICENSE
    }

    private void resolveNoticeFiles() throws MojoExecutionException, IOException {
        resolveArtifactFiles(NOTICE);
    }

    private void resolveLicenseFiles() throws MojoExecutionException, IOException {
        resolveArtifactFiles(LICENSE);
    }

    private void resolveArtifactFiles(final EmbeddedArtifact artifact) throws MojoExecutionException, IOException {
        final String name;
        final ProjectFlag ignoreFlag;
        final ProjectFlag alternateFilenameFlag;
        final ProjectFlag onMultipleFlag;
        final Predicate<JarEntry> filter;
        final BiConsumer<Project, String> consumer;
        final UnaryOperator<String> contentTransformer;

        switch (artifact) {
            case NOTICE:
                name = "NOTICE";
                ignoreFlag = IGNORE_MISSING_EMBEDDED_NOTICE;
                alternateFilenameFlag = ALTERNATE_NOTICE_FILE;
                onMultipleFlag = ON_MULTIPLE_EMBEDDED_NOTICE;
                // TODO(mblow): this will match *any* NOTICE[.(txt|md)] file located within the artifact-
                // this seems way too liberal
                filter = entry -> entry.getName().matches("(.*/|^)" + "NOTICE" + "(.(txt|md))?");
                consumer = Project::setNoticeText;
                contentTransformer = getNoticeFileContentTransformer();
                break;
            case LICENSE:
                name = "LICENSE";
                ignoreFlag = IGNORE_MISSING_EMBEDDED_LICENSE;
                alternateFilenameFlag = ALTERNATE_LICENSE_FILE;
                onMultipleFlag = ON_MULTIPLE_EMBEDDED_LICENSE;
                // TODO(mblow): this will match *any* LICENSE[.(txt|md)] file located within the artifact-
                // this seems way too liberal
                filter = entry -> entry.getName().matches("(.*/|^)" + "LICENSE" + "(.(txt|md))?");
                consumer = Project::setLicenseText;
                contentTransformer = UnaryOperator.identity();
                break;
            default:
                throw new IllegalStateException("NYI: " + artifact);
        }
        for (Project p : getProjects()) {
            File artifactFile = new File(p.getArtifactPath());
            if (!artifactFile.exists()) {
                throw new MojoExecutionException("Artifact file " + artifactFile + " does not exist!");
            } else if (!artifactFile.getName().endsWith(".jar")) {
                getLog().info("Skipping unknown artifact file type: " + artifactFile);
                continue;
            } else if (!validateShadowLicenses && p.isShadowed()) {
                getLog().info("Skipping shadowed project: " + p.gav());
                continue;
            }
            String alternateFilename = (String) getProjectFlag(p.gav(), alternateFilenameFlag);
            Predicate<JarEntry> finalFilter =
                    alternateFilename != null ? entry -> entry.getName().equals(alternateFilename) : filter;
            try (JarFile jarFile = new JarFile(artifactFile)) {
                SortedMap<String, JarEntry> matches = gatherMatchingEntries(jarFile, finalFilter);
                if (matches.isEmpty()) {
                    warnUnlessFlag(p, ignoreFlag, "No " + name + " file found for " + p.gav());
                } else {
                    if (matches.size() > 1) {
                        // TODO(mblow): duplicate elimination on matches content
                        warnUnlessFlag(p, onMultipleFlag,
                                "Multiple " + name + " files found for " + p.gav() + ": " + matches.keySet() + "!");
                        String onMultiple = (String) getProjectFlag(p.gav(), onMultipleFlag);
                        if (onMultiple == null) {
                            onMultiple = "concat";
                        }
                        switch (onMultiple.toLowerCase()) {
                            case "concat":
                                getLog().info("...concatenating all " + matches.size() + " matches");
                                StringBuilder content = new StringBuilder();
                                for (Map.Entry<String, JarEntry> match : matches.entrySet()) {
                                    resolveContent(p, jarFile, match.getValue(), contentTransformer, (p1, text) -> {
                                        content.append("------------ BEGIN <").append(match.getKey())
                                                .append("> ------------\n");
                                        content.append(text);
                                        if (content.charAt(content.length() - 1) != '\n') {
                                            content.append('\n');
                                        }
                                        content.append("------------ END <").append(match.getKey())
                                                .append("> ------------\n");
                                    }, name);
                                }
                                consumer.accept(p, content.toString());
                                break;
                            case "first":
                                Map.Entry<String, JarEntry> first = matches.entrySet().iterator().next();
                                getLog().info("...taking first match: " + first.getKey());
                                resolveContent(p, jarFile, first.getValue(), contentTransformer, consumer, name);
                                break;
                            default:
                                throw new IllegalArgumentException("unknown value for " + onMultipleFlag.propName()
                                        + ": " + onMultiple.toLowerCase());
                        }
                    } else {
                        Map.Entry<String, JarEntry> match = matches.entrySet().iterator().next();
                        getLog().info(p.gav() + " has " + name + " file: " + match.getKey());
                        resolveContent(p, jarFile, match.getValue(), contentTransformer, consumer, name);
                    }
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

    private UnaryOperator<String> getNoticeFileContentTransformer() {
        UnaryOperator<String> transformer;
        if (stripFoundationAssertionFromNotices) {
            transformer = text -> FOUNDATION_PATTERN.matcher(text).replaceAll("");
        } else {
            transformer = UnaryOperator.identity();
        }
        return transformer;
    }

    @java.lang.Override
    protected void gatherProjectDependencies(MavenProject project,
            Map<MavenProject, List<Pair<String, String>>> dependencyLicenseMap,
            Map<String, MavenProject> dependencyGavMap) throws ProjectBuildingException, MojoExecutionException {
        super.gatherProjectDependencies(project, dependencyLicenseMap, dependencyGavMap);
        gatherShadowedDependencies(dependencyLicenseMap, dependencyGavMap);
    }

    @java.lang.Override
    protected void processExtraDependencies(Map<MavenProject, List<Pair<String, String>>> dependencyLicenseMap,
            Map<String, MavenProject> dependencyGavMap) throws ProjectBuildingException, MojoExecutionException {
        super.processExtraDependencies(dependencyLicenseMap, dependencyGavMap);
        gatherShadowedDependencies(dependencyLicenseMap, dependencyGavMap);
    }

    private void gatherShadowedDependencies(Map<MavenProject, List<Pair<String, String>>> dependencyLicenseMap,
            Map<String, MavenProject> dependencyGavMap) throws MojoExecutionException, ProjectBuildingException {
        if (!includeShadowedDependencies) {
            getLog().info("Not gathering shadowed dependencies as 'includeShadowedDependencies' is set to "
                    + includeShadowedDependencies);
            return;
        }
        Set<MavenProject> projects = new TreeSet<>(Comparator.comparing(MavenProject::getId));
        projects.addAll(dependencyLicenseMap.keySet());
        for (MavenProject p : projects) {
            File artifactFile = p.getArtifact().getFile();
            if (!artifactFile.exists()) {
                throw new MojoExecutionException("Artifact file " + artifactFile + " does not exist!");
            } else if (!artifactFile.getName().endsWith(".jar")) {
                getLog().info("Skipping unknown artifact file type: " + artifactFile);
                continue;
            }
            @SuppressWarnings("unchecked")
            List<String[]> specs = (List<String[]>) getProjectFlags()
                    .getOrDefault(Pair.of(toGav(p), IGNORE_SHADOWED_DEPENDENCIES), Collections.emptyList());
            getLog().debug(p + " has " + IGNORE_SHADOWED_DEPENDENCIES.propName() + " set to "
                    + specs.stream().map(ArrayUtils::toString).collect(Collectors.joining(",")));
            try (JarFile jarFile = new JarFile(artifactFile)) {
                SortedMap<String, JarEntry> matches = gatherMatchingEntries(jarFile,
                        entry -> entry.getName().matches("(.*/|^)" + "pom\\.properties"));
                if (!matches.isEmpty()) {
                    jarEntryLoop: for (JarEntry entry : matches.values()) {
                        Properties props = new Properties();
                        props.load(jarFile.getInputStream(entry));
                        String groupId = props.getProperty("groupId");
                        String artifactId = props.getProperty("artifactId");
                        String version = props.getProperty("version");
                        String gav = groupId + ":" + artifactId + ":" + version;
                        if (!dependencyGavMap.containsKey(gav)) {
                            for (String[] ignoreSpec : specs) {
                                if ((ignoreSpec[0].equals(groupId) || ignoreSpec[0].equals("*"))
                                        && (ignoreSpec[1].equals(artifactId) || ignoreSpec[1].equals("*"))
                                        && (ignoreSpec[2].equals(version) || ignoreSpec[2].equals("*"))) {
                                    getLog().info("skipping " + gav + " (shadowed from " + p.getId()
                                            + "), as it matches " + IGNORE_SHADOWED_DEPENDENCIES.propName());
                                    continue jarEntryLoop;
                                }
                            }
                            getLog().info("adding " + gav + " (shadowed from " + p.getId() + ")");
                            ArtifactHandler handler = new DefaultArtifactHandler("jar");
                            String[] gavParts = StringUtils.split(gav, ':');
                            Artifact manualDep = new DefaultArtifact(gavParts[0], gavParts[1], gavParts[2],
                                    Artifact.SCOPE_COMPILE, "jar", null, handler);
                            processArtifact(manualDep, dependencyLicenseMap, dependencyGavMap, true);
                        }
                    }
                }
            } catch (IOException e) {
                throw new MojoExecutionException(e);
            }
        }
    }
}
