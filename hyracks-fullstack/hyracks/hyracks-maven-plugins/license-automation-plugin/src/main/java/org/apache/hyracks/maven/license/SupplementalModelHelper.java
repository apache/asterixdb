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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugin.resources.remote.Supplement;
import org.apache.maven.plugin.resources.remote.SupplementalDataModel;
import org.apache.maven.plugin.resources.remote.io.xpp3.SupplementalDataModelXpp3Reader;
import org.apache.maven.project.inheritance.ModelInheritanceAssembler;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;

public class SupplementalModelHelper {

    private SupplementalModelHelper() {
    }

    // following code taken from ProcessRemoteResourcesMojo (org.apache.maven.plugins:maven-remote-resources-plugin:1.5)

    static String generateSupplementMapKey(String groupId, String artifactId) {
        return groupId.trim() + ":" + artifactId.trim();
    }

    static Map<String, Model> loadSupplements(Log log, String[] models) throws MojoExecutionException {
        if (models == null) {
            log.debug("Supplemental data models won't be loaded.  " + "No models specified.");
            return Collections.emptyMap();
        }

        List<Supplement> supplements = new ArrayList<>();
        for (String set : models) {
            log.debug("Preparing ruleset: " + set);
            try {
                File f = new File(set);

                if (!f.exists()) {
                    throw new MojoExecutionException("Cold not resolve " + set);
                }
                if (!f.canRead()) {
                    throw new MojoExecutionException("Supplemental data models won't be loaded. " + "File "
                            + f.getAbsolutePath() + " cannot be read, check permissions on the file.");
                }

                log.debug("Loading supplemental models from " + f.getAbsolutePath());

                SupplementalDataModelXpp3Reader reader = new SupplementalDataModelXpp3Reader();
                try (FileInputStream fis = new FileInputStream(f); Reader fileReader = new InputStreamReader(fis)) {
                    SupplementalDataModel supplementalModel = reader.read(fileReader);
                    supplements.addAll(supplementalModel.getSupplement());
                }
            } catch (Exception e) {
                String msg = "Error loading supplemental data models: " + e.getMessage();
                log.error(msg, e);
                throw new MojoExecutionException(msg, e);
            }
        }

        log.debug("Loading supplements complete.");

        Map<String, Model> supplementMap = new HashMap<>();
        for (Supplement sd : supplements) {
            Xpp3Dom dom = (Xpp3Dom) sd.getProject();

            Model m = getSupplement(log, dom);
            supplementMap.put(generateSupplementMapKey(m.getGroupId(), m.getArtifactId()), m);
        }

        return supplementMap;
    }

    protected static Model getSupplement(Log log, Xpp3Dom supplementModelXml) throws MojoExecutionException {
        MavenXpp3Reader modelReader = new MavenXpp3Reader();
        Model model = null;

        try {
            model = modelReader.read(new StringReader(supplementModelXml.toString()));
            String groupId = model.getGroupId();
            String artifactId = model.getArtifactId();

            if (groupId == null || "".equals(groupId.trim())) {
                throw new MojoExecutionException(
                        "Supplemental project XML requires that a <groupId> element be present.");
            }

            if (artifactId == null || "".equals(artifactId.trim())) {
                throw new MojoExecutionException(
                        "Supplemental project XML requires that a <artifactId> element be present.");
            }
        } catch (IOException e) {
            log.warn("Unable to read supplemental XML: " + e.getMessage(), e);
        } catch (XmlPullParserException e) {
            log.warn("Unable to parse supplemental XML: " + e.getMessage(), e);
        }

        return model;
    }

    protected static Model mergeModels(ModelInheritanceAssembler assembler, Model parent, Model child) {
        assembler.assembleModelInheritance(child, parent);
        // ModelInheritanceAssembler doesn't push the name, do it here
        if (child.getName() == null) {
            child.setName(parent.getName());
        }
        return child;
    }

}
