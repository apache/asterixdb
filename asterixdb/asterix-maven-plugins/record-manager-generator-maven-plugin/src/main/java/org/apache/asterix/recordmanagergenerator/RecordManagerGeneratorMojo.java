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

package org.apache.asterix.recordmanagergenerator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.project.MavenProject;

/**
 * @goal generate-record-manager
 * @phase generate-sources
 * @requiresDependencyResolution compile
 */
public class RecordManagerGeneratorMojo extends AbstractMojo {

    /**
     * parameter injected from pom.xml
     *
     * @parameter
     */
    private boolean debug;
    /**
     * parameter injected from pom.xml
     *
     * @parameter
     * @required
     */
    private String packageName;
    /**
     * parameter injected from pom.xml
     *
     * @parameter
     * @required
     */
    private File[] inputFiles;
    /**
     * @parameter default-value="${project}"
     * @required
     * @readonly
     */
    MavenProject project;

    String recordManagerTemplate = "RecordManager.java";
    String arenaManagerTemplate = "ArenaManager.java";
    String[] supportTemplates = { "RecordManagerStats.java", "AllocInfo.java", "TypeUtil.java" };

    private Map<String, RecordType> typeMap;

    public RecordManagerGeneratorMojo() {
    }

    private void readRecordTypes() throws MojoExecutionException {
        if (debug) {
            getLog().info("generating debug code");
        }

        typeMap = new HashMap<String, RecordType>();

        for (int i = 0; i < inputFiles.length; ++i) {
            getLog().info("reading " + inputFiles[i].toString());
            try (Reader read = new FileReader(inputFiles[i])) {
                RecordType type = RecordType.read(read);
                // always add allocId to enable tracking of allocations
                type.addField("alloc id", RecordType.Type.SHORT, null);
                type.addToMap(typeMap);
            } catch (FileNotFoundException fnfe) {
                throw new MojoExecutionException("could not find type description file " + inputFiles[i], fnfe);
            } catch (IOException e) {
                throw new MojoExecutionException("error closing type description file " + inputFiles[i], e);
            }
        }
    }

    public void execute() throws MojoExecutionException, MojoFailureException {
        String outputPath = project.getBuild().getDirectory() + File.separator + "generated-sources" + File.separator
                + "java" + File.separator + packageName.replace('.', File.separatorChar);
        File dir = new File(outputPath);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        readRecordTypes();

        for (String recordType : typeMap.keySet()) {
            generateSource(Generator.TemplateType.RECORD_MANAGER, recordManagerTemplate, recordType, outputPath);
            generateSource(Generator.TemplateType.ARENA_MANAGER, arenaManagerTemplate, recordType, outputPath);
        }

        for (int i = 0; i < supportTemplates.length; ++i) {
            generateSource(Generator.TemplateType.SUPPORT, supportTemplates[i], "", outputPath);
        }
    }

    private void generateSource(Generator.TemplateType mgrType, String template, String recordType, String outputPath)
            throws MojoFailureException {
        InputStream is = getClass().getClassLoader().getResourceAsStream(template);
        if (is == null) {
            throw new MojoFailureException("template '" + template + "' not found in classpath");
        }

        StringBuilder sb = new StringBuilder();
        File outputFile = new File(outputPath + File.separator + recordType + template);

        try {
            getLog().info("generating " + outputFile.toString());

            Generator.generateSource(mgrType, packageName, typeMap.get(recordType), is, sb, debug);
            is.close();

            FileWriter outWriter = new FileWriter(outputFile);
            outWriter.write(sb.toString());
            outWriter.close();
        } catch (Exception ex) {
            getLog().error(ex);
            throw new MojoFailureException("failed to generate " + outputFile.toString());
        }
    }
}
