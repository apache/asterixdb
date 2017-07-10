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

package org.apache.asterix.runtime.evaluators.plugin;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Set;

import org.apache.asterix.common.utils.CodeGenHelper;
import org.apache.asterix.runtime.evaluators.staticcodegen.CodeGenUtil;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.project.MavenProject;
import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;

/**
 * Statically generates null-handling byte code for scalar functions.
 *
 * @goal generate-evaluator
 * @phase process-classes
 */
public class EvaluatorGeneratorMojo extends AbstractMojo {

    private String baseDir;

    /**
     * @parameter default-value="${project}"
     * @required
     * @readonly
     */
    MavenProject project;

    /**
     * @parameter default-value="${evaluatorPackagePrefix}"
     * @required
     * @readonly
     */
    String evaluatorPackagePrefix;

    public EvaluatorGeneratorMojo() {
    }

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        baseDir = project.getBuild().getDirectory() + File.separator + "classes";

        URLClassLoader classLoader = null;
        try {
            URI baseURI = new File(baseDir).toURI();
            classLoader = new URLClassLoader(new URL[] { baseURI.toURL() }, getClass().getClassLoader());

            String superClassName = CodeGenHelper.toJdkStandardName(CodeGenUtil.DESCRIPTOR_SUPER_CLASS_NAME);
            Class superClass = Class.forName(superClassName, false, classLoader);

            // Finds all sub-classes of the given root class within the specified package
            ConfigurationBuilder config = ConfigurationBuilder.build(classLoader, evaluatorPackagePrefix);
            String genSuffix = CodeGenHelper.DEFAULT_SUFFIX_FOR_GENERATED_CLASS + ".class";
            config.setInputsFilter(path -> path != null && !path.endsWith(genSuffix));

            Reflections reflections = new Reflections(config);
            Set<Class<?>> allClasses = reflections.getSubTypesOf(superClass);

            // Generates byte code for all sub-classes
            for (Class<?> cl : allClasses) {
                getLog().info("Generating byte code for " + cl.getName());
                CodeGenUtil.generateScalarFunctionDescriptorBinary(evaluatorPackagePrefix, cl.getName(),
                        CodeGenHelper.DEFAULT_SUFFIX_FOR_GENERATED_CLASS, classLoader,
                        (name, bytes) -> writeFile(name, bytes));
            }
        } catch (Exception e) {
            getLog().error(e);
            throw new MojoFailureException(e.toString());
        } finally {
            if (classLoader != null) {
                try {
                    classLoader.close();
                } catch (IOException e) {
                    getLog().error(e);
                }
            }
        }
    }

    private void writeFile(String name, byte[] classDefinitionBinary) throws IOException {
        File targetFile = new File(baseDir + File.separator + name + ".class");
        targetFile.getParentFile().mkdirs();
        targetFile.createNewFile();
        try (FileOutputStream outputStream = new FileOutputStream(targetFile)) {
            outputStream.write(classDefinitionBinary, 0, classDefinitionBinary.length);
        }
    }

}
