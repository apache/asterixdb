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

import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hyracks.maven.license.LicenseSpec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LicensedProjects {
    private LicenseSpec license;

    private SortedSet<Project> projects = new TreeSet<>(Project.PROJECT_COMPARATOR);

    public LicensedProjects(LicenseSpec license) {
        this.license = license;
    }

    @JsonCreator
    public LicensedProjects(@JsonProperty("license") LicenseSpec license,
            @JsonProperty("projects") Set<Project> projects) {
        this.license = license;
        this.projects.addAll(projects);
    }

    public LicenseSpec getLicense() {
        return license;
    }

    public SortedSet<Project> getProjects() {
        return projects;
    }

    public void addProject(Project project) {
        projects.add(project);
    }
}
