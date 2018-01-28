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
package org.apache.hyracks.maven.plugin;

import java.io.File;
import java.io.IOException;

import org.apache.asterix.testframework.template.TemplateHelper;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.project.MavenProject;
import org.apache.maven.shared.model.fileset.FileSet;
import org.apache.maven.shared.model.fileset.util.FileSetManager;

/**
 * @goal generate-testdata
 *
 * @phase process-test-resources
 */
public class TestDataGeneratorMojo extends AbstractMojo {

    /**
     * @parameter default-value="${project}"
     * @required
     * @readonly
     */
    MavenProject project;

    /**
     * @parameter
     * @required
     */
    FileSet inputFiles;

    /**
     * @parameter
     * @required
     */
    File outputDir;

    /**
     * @parameter default-value="${maven.test.skip}"
     */
    boolean skip;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        if (skip) {
            getLog().info("Skipping plugin execution (configured skip)");
            return;
        }
        FileSetManager mgr = new FileSetManager();
        // this seems pretty hacky, but necessary to get the correct result.
        File inputFilesDirectory = new File(inputFiles.getDirectory());
        if (!inputFilesDirectory.isAbsolute()) {
            inputFiles.setDirectory(new File(project.getBasedir(), inputFiles.getDirectory()).getAbsolutePath());
        }

        final String[] includedFiles = mgr.getIncludedFiles(inputFiles);
        getLog().info("Processing " + includedFiles.length + " files...");
        for (String file : includedFiles) {
            getLog().info("      -" + file);
            try {
                TemplateHelper.INSTANCE.processFile(new File(inputFiles.getDirectory(), file),
                        new File(outputDir, file.replace(".template", "")));
            } catch (IOException e) {
                e.printStackTrace();
                throw new MojoExecutionException("failure", e);
            }
        }
    }
}
