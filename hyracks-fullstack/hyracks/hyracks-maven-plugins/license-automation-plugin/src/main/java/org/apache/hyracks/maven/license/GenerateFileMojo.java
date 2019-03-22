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

import static org.apache.hyracks.maven.license.ProjectFlag.ALTERNATE_LICENSE_FILE;
import static org.apache.hyracks.maven.license.ProjectFlag.ALTERNATE_NOTICE_FILE;
import static org.apache.hyracks.maven.license.ProjectFlag.IGNORE_MISSING_EMBEDDED_LICENSE;
import static org.apache.hyracks.maven.license.ProjectFlag.IGNORE_MISSING_EMBEDDED_NOTICE;
import static org.apache.hyracks.maven.license.ProjectFlag.IGNORE_NOTICE_OVERRIDE;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

import org.apache.commons.io.IOUtils;
import org.apache.hyracks.maven.license.freemarker.IndentDirective;
import org.apache.hyracks.maven.license.freemarker.LoadFileDirective;
import org.apache.hyracks.maven.license.project.LicensedProjects;
import org.apache.hyracks.maven.license.project.Project;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
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
            resolveArtifactContent(license, true);
        }
    }

    private String resolveArtifactContent(ArtifactSpec artifact, boolean bestEffort) throws IOException {
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
                    getLog().warn("MISSING: content file (" + cFile + ") for url: " + artifact.getUrl());
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
                    warnUnlessFlag(p.gav(), IGNORE_MISSING_EMBEDDED_LICENSE,
                            "Using license other than from within artifact: " + p.gav() + " (" + lps.getLicense()
                                    + ")");
                    licenseText = resolveArtifactContent(lps.getLicense(), false);
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

    private void buildNoticeProjectMap() throws IOException {
        noticeMap = new TreeMap<>(WHITESPACE_NORMALIZED_COMPARATOR);
        for (Project p : getProjects()) {
            String noticeText = p.getNoticeText();
            if (noticeText == null && noticeOverrides.containsKey(p.gav())) {
                String noticeUrl = noticeOverrides.get(p.gav());
                warnUnlessFlag(p.gav(), IGNORE_NOTICE_OVERRIDE,
                        "Using notice other than from within artifact: " + p.gav() + " (" + noticeUrl + ")");
                p.setNoticeText(resolveArtifactContent(new NoticeSpec(noticeUrl), false));
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

    private void resolveNoticeFiles() throws MojoExecutionException, IOException {
        // TODO(mblow): this will match *any* NOTICE[.txt] file located within the artifact- this seems way too liberal
        resolveArtifactFiles("NOTICE", IGNORE_MISSING_EMBEDDED_NOTICE, ALTERNATE_NOTICE_FILE,
                entry -> entry.getName().matches("(.*/|^)" + "NOTICE" + "(.txt)?"), Project::setNoticeText,
                text -> stripFoundationAssertionFromNotices ? FOUNDATION_PATTERN.matcher(text).replaceAll("") : text);
    }

    private void resolveLicenseFiles() throws MojoExecutionException, IOException {
        // TODO(mblow): this will match *any* LICENSE[.txt] file located within the artifact- this seems way too liberal
        resolveArtifactFiles("LICENSE", IGNORE_MISSING_EMBEDDED_LICENSE, ALTERNATE_LICENSE_FILE,
                entry -> entry.getName().matches("(.*/|^)" + "LICENSE" + "(.txt)?"), Project::setLicenseText,
                UnaryOperator.identity());
    }

    private void resolveArtifactFiles(final String name, final ProjectFlag ignoreFlag,
            final ProjectFlag alternateFilenameFlag, final Predicate<JarEntry> filter,
            final BiConsumer<Project, String> consumer, final UnaryOperator<String> contentTransformer)
            throws MojoExecutionException, IOException {
        for (Project p : getProjects()) {
            File artifactFile = new File(p.getArtifactPath());
            if (!artifactFile.exists()) {
                throw new MojoExecutionException("Artifact file " + artifactFile + " does not exist!");
            } else if (!artifactFile.getName().endsWith(".jar")) {
                getLog().info("Skipping unknown artifact file type: " + artifactFile);
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
                        getLog().warn("Multiple " + name + " files found for " + p.gav() + ": " + matches.keySet()
                                + "; taking first");
                    } else {
                        getLog().info(p.gav() + " has " + name + " file: " + matches.keySet());
                    }
                    resolveContent(p, jarFile, matches.values().iterator().next(), contentTransformer, consumer, name);
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
}
