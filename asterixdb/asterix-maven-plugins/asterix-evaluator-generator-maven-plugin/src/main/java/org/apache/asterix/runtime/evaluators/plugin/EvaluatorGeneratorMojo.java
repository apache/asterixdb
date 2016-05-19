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
import java.util.Set;

import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.staticcodegen.CodeGenUtil;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.project.MavenProject;
import org.reflections.Reflections;

/**
 * Statically generates null-handling byte code for scalar functions.
 *
 * @goal generate-evaluator
 * @phase compile
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
        try {
            // Finds all sub-classes of AbstractScalarFunctionDynamicDescriptor with in the package
            // org.apache.asterix.runtime.evaluators.
            Reflections reflections = new Reflections(evaluatorPackagePrefix);
            Set<Class<? extends AbstractScalarFunctionDynamicDescriptor>> allClasses = reflections
                    .getSubTypesOf(AbstractScalarFunctionDynamicDescriptor.class);

            // Generates byte code for all sub-classes of AbstractScalarFunctionDynamicDescriptor.
            for (Class<?> cl : allClasses) {
                getLog().info("Generating byte code for " + cl.getName());
                CodeGenUtil.generateScalarFunctionDescriptorBinary(evaluatorPackagePrefix, cl.getName(),
                        CodeGenUtil.DEFAULT_SUFFIX_FOR_GENERATED_CLASS, reflections.getClass().getClassLoader(),
                        (name, bytes) -> writeFile(name, bytes));
            }
        } catch (Exception e) {
            getLog().error(e);
            throw new MojoFailureException(e.toString());
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
