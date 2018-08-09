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
package org.apache.hyracks.maven.license.project;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.maven.project.MavenProject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Project {
    private String name;
    private String groupId;
    private String artifactId;
    private String url;
    private String version;
    private String location;
    private String artifactPath;
    private String noticeText;
    private String licenseText;
    private String sourcePointer;
    private String classifier;

    @JsonIgnore
    private MavenProject mavenProject;

    public static final Comparator<Project> PROJECT_COMPARATOR =
            (o1, o2) -> o1.compareToken().compareTo(o2.compareToken());

    public Project(MavenProject project, String location, File artifactPath) {
        mavenProject = project;
        name = project.getName();
        groupId = project.getGroupId();
        artifactId = project.getArtifactId();
        version = project.getVersion();
        url = project.getUrl();
        classifier = project.getArtifact().getClassifier();
        this.artifactPath = artifactPath.getPath();
        setLocation(location);
    }

    @JsonCreator
    public Project(@JsonProperty("name") String name, @JsonProperty("groupId") String groupId,
            @JsonProperty("artifactId") String artifactId, @JsonProperty("url") String url,
            @JsonProperty("version") String version, @JsonProperty("location") String location,
            @JsonProperty("artifactPath") String artifactPath, @JsonProperty("noticeText") String noticeText,
            @JsonProperty("licenseText") String licenseText, @JsonProperty("classifier") String classifier) {
        this.name = name;
        this.groupId = groupId;
        this.artifactId = artifactId;
        this.url = url;
        this.version = version;
        this.location = location;
        this.artifactPath = artifactPath;
        this.noticeText = noticeText;
        this.licenseText = licenseText;
        this.classifier = classifier;
    }

    public String getName() {
        return name;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getArtifactId() {
        return artifactId;
    }

    public String getUrl() {
        return url;
    }

    public String getClassifier() {
        return classifier;
    }

    public String getVersion() {
        return version;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        if (location != null && !location.endsWith("/")) {
            this.location = location + "/";
        } else {
            this.location = location;
        }
    }

    @JsonIgnore
    public String getJarName() {
        return artifactId + "-" + version + (classifier != null ? "-" + classifier : "") + ".jar";
    }

    @JsonIgnore
    public List<String> getLocations() {
        // TODO(mblow): store locations as an set instead of string
        return Arrays.asList(getLocation().split(","));
    }

    public void setArtifactId(String artifactId) {
        this.artifactId = artifactId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setClassifier(String classifier) {
        this.classifier = classifier;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getArtifactPath() {
        return artifactPath;
    }

    public void setNoticeText(String noticeText) {
        this.noticeText = noticeText;
    }

    public String getNoticeText() {
        return noticeText;
    }

    public String gav() {
        return getGroupId() + ":" + getArtifactId() + ":" + getVersion();
    }

    private String compareToken() {
        return getName() + getArtifactId() + getVersion() + ":" + getLocation();
    }

    public String getLicenseText() {
        return licenseText;
    }

    public void setLicenseText(String licenseText) {
        this.licenseText = licenseText;
    }

    public String getSourcePointer() {
        return sourcePointer;
    }

    public void setSourcePointer(String sourcePointer) {
        this.sourcePointer = sourcePointer;
    }

    @Override
    public String toString() {
        return "Project [" + gav() + "]";
    }
}
