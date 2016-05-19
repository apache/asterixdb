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

package org.apache.asterix.runtime.evaluators.staticcodegen;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;

/**
 * A utility class that generates byte code for scalar function descriptors.
 */
public class CodeGenUtil {

    public final static String DEFAULT_SUFFIX_FOR_GENERATED_CLASS = "Gen";
    private final static String OBJECT_CLASS_NAME = "java/lang/Object";
    private final static String EVALUATOR_FACTORY = "EvaluatorFactory";
    private final static String EVALUATOR = "Evaluator";
    private final static String INNER = "Inner";
    private final static String DESCRIPTOR = "Descriptor";
    private final static String DOLLAR = "$";

    /**
     * The callback interface for a caller to determine what it needs to do for
     * the generated class bytes.
     */
    public static interface ClassByteCodeAction {

        /**
         * Run a user-defined action for the generated class definition bytes.
         *
         * @param targetClassName,
         *            the name for the generated class.
         * @param classDefinitionBytes,
         *            the class definition bytes.
         * @throws IOException
         */
        public void runAction(String targetClassName, byte[] classDefinitionBytes) throws IOException;
    };

    /**
     * Generates the byte code for a scalar function descriptor.
     *
     * @param packagePrefix,
     *            the prefix of evaluators for code generation.
     * @param originalFuncDescriptorClassName,
     *            the original class name of the function descriptor.
     * @param suffixForGeneratedClass,
     *            the suffix for the generated class.
     * @param action,
     *            the customized action for the generated class definition bytes.
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static void generateScalarFunctionDescriptorBinary(String packagePrefix,
            String originalFuncDescriptorClassName, String suffixForGeneratedClass, ClassLoader classLoader,
            ClassByteCodeAction action) throws IOException, ClassNotFoundException {
        originalFuncDescriptorClassName = toInternalClassName(originalFuncDescriptorClassName);
        String targetFuncDescriptorClassName = toInternalClassName(
                originalFuncDescriptorClassName + suffixForGeneratedClass);

        // Adds the mapping of the old/new names of the function descriptor.
        List<Pair<String, String>> nameMappings = new ArrayList<>();
        nameMappings.add(Pair.of(originalFuncDescriptorClassName, targetFuncDescriptorClassName));
        nameMappings.add(Pair.of(toJdkStandardName(originalFuncDescriptorClassName),
                toJdkStandardName(targetFuncDescriptorClassName)));

        // Gathers evaluator factory classes that are created in the function descriptor.
        ClassReader reader = new ClassReader(getResourceStream(originalFuncDescriptorClassName, classLoader));
        GatherEvaluatorFactoryCreationVisitor evalFactoryCreationVisitor = new GatherEvaluatorFactoryCreationVisitor(
                toInternalClassName(packagePrefix));
        reader.accept(evalFactoryCreationVisitor, 0);
        Set<String> evaluatorFactoryClassNames = evalFactoryCreationVisitor.getCreatedEvaluatorFactoryClassNames();

        // Generates inner classes other than evaluator factories.
        generateNonEvalInnerClasses(reader, evaluatorFactoryClassNames, nameMappings, suffixForGeneratedClass,
                classLoader, action);

        // Generates evaluator factories that are created in the function descriptor.
        int evalFactoryCounter = 0;
        for (String evaluateFactoryClassName : evaluatorFactoryClassNames) {
            generateEvaluatorFactoryClassBinary(packagePrefix, evaluateFactoryClassName, suffixForGeneratedClass,
                    ++evalFactoryCounter, nameMappings, classLoader, action);
        }

        // Transforms the function descriptor class and outputs the generated class binary.
        ClassWriter writer = new ClassWriter(reader, 0);
        RenameClassVisitor renamingVisitor = new RenameClassVisitor(writer, nameMappings);
        reader.accept(renamingVisitor, 0);
        action.runAction(targetFuncDescriptorClassName, writer.toByteArray());
    }

    /**
     * Apply mappings for a class name.
     *
     * @param nameMappings,
     *            the mappings from existing class names to that of their generated counterparts.
     * @param inputStr,
     *            the name of a class.
     * @return the name of the generated counterpart class.
     */
    static String applyMapping(List<Pair<String, String>> nameMappings, String inputStr) {
        if (inputStr == null) {
            return null;
        }
        String result = inputStr;

        // Applies name mappings in the reverse order, i.e.,
        // mapping recent added old/new name pairs first.
        int index = nameMappings.size() - 1;
        for (; index >= 0; --index) {
            Pair<String, String> entry = nameMappings.get(index);
            result = result.replace(entry.getLeft(), entry.getRight());
        }
        return result;
    }

    /**
     * Generates the byte code for an evaluator factory class.
     *
     * @param packagePrefix,
     *            the prefix of evaluators for code generation.
     * @param originalEvaluatorFactoryClassName,
     *            the original evaluator factory class name.
     * @param suffixForGeneratedClass,
     *            the suffix for the generated class.
     * @param factoryIndex,
     *            the counter for the generated class.
     * @param nameMappings,
     *            class names that needs to be rewritten in the generated byte code.
     * @param classLoader,
     *            a class loader that has the original evaluator factory class in its resource paths.
     * @param action,
     *            a user definition action for the generated byte code.
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private static void generateEvaluatorFactoryClassBinary(String packagePrefix,
            String originalEvaluatorFactoryClassName, String suffixForGeneratedClass, int factoryCounter,
            List<Pair<String, String>> nameMappings, ClassLoader classLoader, ClassByteCodeAction action)
            throws IOException, ClassNotFoundException {
        originalEvaluatorFactoryClassName = toInternalClassName(originalEvaluatorFactoryClassName);
        String targetEvaluatorFactoryClassName = getGeneratedClassName(originalEvaluatorFactoryClassName,
                EVALUATOR_FACTORY + suffixForGeneratedClass, factoryCounter);

        // Adds the old/new names of the evaluator factory into the mapping.
        nameMappings.add(Pair.of(originalEvaluatorFactoryClassName, targetEvaluatorFactoryClassName));
        nameMappings.add(Pair.of(toJdkStandardName(originalEvaluatorFactoryClassName),
                toJdkStandardName(targetEvaluatorFactoryClassName)));

        // Gathers the class names of the evaluators that are created in the evaluator factory.
        ClassReader reader = new ClassReader(getResourceStream(originalEvaluatorFactoryClassName, classLoader));
        GatherEvaluatorCreationVisitor evalCreationVisitor = new GatherEvaluatorCreationVisitor(
                toInternalClassName(packagePrefix));
        reader.accept(evalCreationVisitor, 0);
        Set<String> evaluatorClassNames = evalCreationVisitor.getCreatedEvaluatorClassNames();

        // Generates inner classes other than evaluators.
        generateNonEvalInnerClasses(reader, evaluatorClassNames, nameMappings, suffixForGeneratedClass, classLoader,
                action);

        // Generates code for all evaluators.
        int evalCounter = 0;
        for (String evaluateClassName : evaluatorClassNames) {
            generateEvaluatorClassBinary(evaluateClassName, suffixForGeneratedClass, ++evalCounter, nameMappings,
                    classLoader, action);
        }

        // Transforms the evaluator factory class and outputs the generated class binary.
        ClassWriter writer = new ClassWriter(reader, 0);
        RenameClassVisitor renamingVisitor = new RenameClassVisitor(writer, nameMappings);
        reader.accept(renamingVisitor, 0);
        action.runAction(targetEvaluatorFactoryClassName, writer.toByteArray());
    }

    /**
     * Generates the byte code for an evaluator class.
     *
     * @param originalEvaluatorClassName,
     *            the name of the original evaluator class.
     * @param suffixForGeneratedClass,
     *            the suffix for the generated class.
     * @param evalIndex,
     *            the counter for the generated class.
     * @param nameMappings,
     *            class names that needs to be rewritten in the generated byte code.
     * @param classLoader,
     *            a class loader that has the original evaluator factory class in its resource paths.
     * @param action,
     *            a user definition action for the generated byte code.
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private static void generateEvaluatorClassBinary(String originalEvaluatorClassName, String suffixForGeneratedClass,
            int evalCounter, List<Pair<String, String>> nameMappings, ClassLoader classLoader,
            ClassByteCodeAction action) throws IOException, ClassNotFoundException {
        // Convert class names.
        originalEvaluatorClassName = toInternalClassName(originalEvaluatorClassName);
        if (originalEvaluatorClassName.equals(OBJECT_CLASS_NAME)) {
            return;
        }
        String targetEvaluatorClassName = getGeneratedClassName(originalEvaluatorClassName,
                EVALUATOR + suffixForGeneratedClass, evalCounter);

        // Generates code for super classes except java.lang.Object.
        Class<?> evaluatorClass = CodeGenUtil.class.getClassLoader()
                .loadClass(toJdkStandardName(originalEvaluatorClassName));
        generateEvaluatorClassBinary(evaluatorClass.getSuperclass().getName(), suffixForGeneratedClass, evalCounter,
                nameMappings, classLoader, action);

        // Adds name mapping.
        nameMappings.add(Pair.of(originalEvaluatorClassName, targetEvaluatorClassName));
        nameMappings.add(
                Pair.of(toJdkStandardName(originalEvaluatorClassName), toJdkStandardName(targetEvaluatorClassName)));

        // Injects null-handling byte code and output the class binary.
        ClassReader reader = new ClassReader(getResourceStream(originalEvaluatorClassName, classLoader));
        ClassWriter writer = new ClassWriter(reader, 0);
        RenameClassVisitor renamingVisitor = new RenameClassVisitor(writer, nameMappings);
        EvaluatorVisitor evaluateVisitor = new EvaluatorVisitor(renamingVisitor);
        reader.accept(evaluateVisitor, 0);
        action.runAction(targetEvaluatorClassName, writer.toByteArray());
    }

    /**
     * Generates non-evaluator(-factory) inner classes defined in either a function descriptor
     * or an evaluator factory.
     *
     * @param reader,
     *            the reader of the outer class.
     * @param evalClassNames,
     *            the names of evaluator/evaluator-factory classes that shouldn't be generated in this
     *            method.
     * @param nameMappings,
     *            class names that needs to be rewritten in the generated byte code.
     * @param classLoader,
     *            a class loader that has the original evaluator factory class in its resource paths.
     * @param action,
     *            a user definition action for the generated byte code.
     * @throws IOException
     */
    private static void generateNonEvalInnerClasses(ClassReader reader, Set<String> evalClassNames,
            List<Pair<String, String>> nameMappings, String suffixForGeneratedClass, ClassLoader classLoader,
            ClassByteCodeAction action) throws IOException {
        // Gathers inner classes of the function descriptor.
        GatherInnerClassVisitor innerClassVisitor = new GatherInnerClassVisitor();
        reader.accept(innerClassVisitor, 0);
        Set<String> innerClassNames = innerClassVisitor.getInnerClassNames();
        innerClassNames.removeAll(evalClassNames);

        // Rewrites inner classes.
        int counter = 0;
        String suffix = INNER + suffixForGeneratedClass;
        for (String innerClassName : innerClassNames) {
            // adds name mapping.
            String targetInnerClassName = getGeneratedClassName(innerClassName, suffix, ++counter);
            nameMappings.add(Pair.of(innerClassName, targetInnerClassName));
            nameMappings.add(Pair.of(toJdkStandardName(innerClassName), toJdkStandardName(targetInnerClassName)));

            // Renaming appearances of original class names.
            ClassReader innerClassReader = new ClassReader(getResourceStream(innerClassName, classLoader));
            ClassWriter writer = new ClassWriter(innerClassReader, 0);
            RenameClassVisitor renamingVisitor = new RenameClassVisitor(writer, nameMappings);
            innerClassReader.accept(renamingVisitor, 0);
            action.runAction(targetInnerClassName, writer.toByteArray());
        }
    }

    /**
     * Converts a JDK class name to the class naming format of ASM.
     *
     * @param name,
     *            a class name following the JDK convention.
     * @return a "/"-separated class name assumed by ASM.
     */
    private static String toInternalClassName(String name) {
        return name.replace(".", "/");
    }

    /**
     * Converts an ASM class name to the JDK class naming format.
     *
     * @param name,
     *            a class name following the ASM convention.
     * @return a "."-separated class name for JDK.
     */
    private static String toJdkStandardName(String name) {
        return name.replace("/", ".");
    }

    /**
     * Gets the name of a generated class.
     *
     * @param originalClassName,
     *            the original class, i.e., the source of the generated class.
     * @param suffix,
     *            the suffix for the generated class.
     * @param evalCounter,
     *            a counter that appearing at the end of the name of the generated class.
     * @return the name of the generated class.
     */
    private static String getGeneratedClassName(String originalClassName, String suffix, int counter) {
        StringBuilder sb = new StringBuilder();
        int end = originalClassName.indexOf(DESCRIPTOR);
        if (end < 0) {
            end = originalClassName.indexOf(DOLLAR);
        }
        if (end < 0) {
            end = originalClassName.length() - 1;
        }

        sb.append(originalClassName.substring(0, end));
        sb.append(suffix);
        sb.append(counter);
        return sb.toString();
    }

    /**
     * Gets the input stream from a class file.
     *
     * @param className,
     *            the name of a class.
     * @param classLoader,
     *            the corresponding class loader.
     * @return the input stream.
     */
    private static InputStream getResourceStream(String className, ClassLoader classLoader) {
        return classLoader.getResourceAsStream(className.replace('.', '/') + ".class");
    }
}
