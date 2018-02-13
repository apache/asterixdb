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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.utils.CodeGenHelper;
import org.apache.commons.lang3.tuple.Pair;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;

/**
 * A utility class that generates byte code for scalar function descriptors.
 */
public class CodeGenUtil {

    private final static String OBJECT_CLASS_NAME = "java/lang/Object";
    public final static String DESCRIPTOR_SUPER_CLASS_NAME =
            "org/apache/asterix/runtime/evaluators/base/AbstractScalarFunctionDynamicDescriptor";
    private final static String EVALUATOR_FACTORY = "EvaluatorFactory";
    private final static String EVALUATOR = "Evaluator";
    private final static String INNER = "Inner";

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
    }

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
    public static List<Pair<String, String>> generateScalarFunctionDescriptorBinary(String packagePrefix,
            String originalFuncDescriptorClassName, String suffixForGeneratedClass, ClassLoader classLoader,
            ClassByteCodeAction action) throws IOException, ClassNotFoundException {
        String internalFuncDescriptorClassName = CodeGenHelper.toInternalClassName(originalFuncDescriptorClassName);
        if (internalFuncDescriptorClassName.equals(DESCRIPTOR_SUPER_CLASS_NAME)) {
            return Collections.emptyList();
        }

        String targetFuncDescriptorClassName =
                CodeGenHelper.getGeneratedInternalClassName(internalFuncDescriptorClassName, suffixForGeneratedClass);

        // Adds the mapping of the old/new names of the function descriptor.
        List<Pair<String, String>> nameMappings = new ArrayList<>();

        // Generates code for super classes except java.lang.Object.
        Class<?> evaluatorClass =
                classLoader.loadClass(CodeGenHelper.toJdkStandardName(internalFuncDescriptorClassName));
        nameMappings.addAll(generateScalarFunctionDescriptorBinary(packagePrefix,
                evaluatorClass.getSuperclass().getName(), suffixForGeneratedClass, classLoader, action));

        nameMappings.add(Pair.of(internalFuncDescriptorClassName, targetFuncDescriptorClassName));
        nameMappings.add(Pair.of(CodeGenHelper.toJdkStandardName(internalFuncDescriptorClassName),
                CodeGenHelper.toJdkStandardName(targetFuncDescriptorClassName)));

        // Gathers evaluator factory classes that are created in the function descriptor.
        try (InputStream internalFuncDescriptorStream =
                getResourceStream(internalFuncDescriptorClassName, classLoader)) {
            ClassReader reader = new ClassReader(internalFuncDescriptorStream);
            GatherEvaluatorFactoryCreationVisitor evalFactoryCreationVisitor =
                    new GatherEvaluatorFactoryCreationVisitor(CodeGenHelper.toInternalClassName(packagePrefix));
            reader.accept(evalFactoryCreationVisitor, 0);
            Set<String> evaluatorFactoryClassNames = evalFactoryCreationVisitor.getCreatedEvaluatorFactoryClassNames();

            // Generates inner classes other than evaluator factories.
            generateNonEvalInnerClasses(reader, evaluatorFactoryClassNames, nameMappings, suffixForGeneratedClass,
                    classLoader, action);

            // Generates evaluator factories that are created in the function descriptor.
            int evalFactoryCounter = 0;
            for (String evaluateFactoryClassName : evaluatorFactoryClassNames) {
                generateEvaluatorFactoryClassBinary(packagePrefix, evaluateFactoryClassName, suffixForGeneratedClass,
                        evalFactoryCounter++, nameMappings, classLoader, action);
            }

            // Transforms the function descriptor class and outputs the generated class binary.
            ClassWriter writer = new ClassWriter(reader, 0);
            RenameClassVisitor renamingVisitor = new RenameClassVisitor(writer, nameMappings);
            reader.accept(renamingVisitor, 0);
            action.runAction(targetFuncDescriptorClassName, writer.toByteArray());
            return nameMappings;
        }
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
            if (result.contains(entry.getLeft())) {
                return result.replace(entry.getLeft(), entry.getRight());
            }
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
     * @param factoryCounter,
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
        String internalEvaluatorFactoryClassName = CodeGenHelper.toInternalClassName(originalEvaluatorFactoryClassName);
        String targetEvaluatorFactoryClassName = CodeGenHelper.generateClassName(internalEvaluatorFactoryClassName,
                EVALUATOR_FACTORY + suffixForGeneratedClass, factoryCounter);

        // Adds the old/new names of the evaluator factory into the mapping.
        nameMappings.add(Pair.of(internalEvaluatorFactoryClassName, targetEvaluatorFactoryClassName));
        nameMappings.add(Pair.of(CodeGenHelper.toJdkStandardName(internalEvaluatorFactoryClassName),
                CodeGenHelper.toJdkStandardName(targetEvaluatorFactoryClassName)));

        // Gathers the class names of the evaluators that are created in the evaluator factory.
        try (InputStream internalEvaluatorFactoryStream =
                getResourceStream(internalEvaluatorFactoryClassName, classLoader)) {
            ClassReader reader = new ClassReader(internalEvaluatorFactoryStream);
            GatherEvaluatorCreationVisitor evalCreationVisitor =
                    new GatherEvaluatorCreationVisitor(CodeGenHelper.toInternalClassName(packagePrefix));
            reader.accept(evalCreationVisitor, 0);
            Set<String> evaluatorClassNames = evalCreationVisitor.getCreatedEvaluatorClassNames();

            // Generates inner classes other than evaluators.
            generateNonEvalInnerClasses(reader, evaluatorClassNames, nameMappings, suffixForGeneratedClass, classLoader,
                    action);

            // Generates code for all evaluators.
            int evalCounter = 0;
            for (String evaluateClassName : evaluatorClassNames) {
                generateEvaluatorClassBinary(evaluateClassName, suffixForGeneratedClass, evalCounter++, nameMappings,
                        classLoader, action);
            }

            // Transforms the evaluator factory class and outputs the generated class binary.
            ClassWriter writer = new ClassWriter(reader, 0);
            RenameClassVisitor renamingVisitor = new RenameClassVisitor(writer, nameMappings);
            reader.accept(renamingVisitor, 0);
            action.runAction(targetEvaluatorFactoryClassName, writer.toByteArray());
        }
    }

    /**
     * Generates the byte code for an evaluator class.
     *
     * @param originalEvaluatorClassName,
     *            the name of the original evaluator class.
     * @param suffixForGeneratedClass,
     *            the suffix for the generated class.
     * @param evalCounter,
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
        String internalEvaluatorClassName = CodeGenHelper.toInternalClassName(originalEvaluatorClassName);
        if (internalEvaluatorClassName.equals(OBJECT_CLASS_NAME)) {
            return;
        }
        String targetEvaluatorClassName = CodeGenHelper.generateClassName(internalEvaluatorClassName,
                EVALUATOR + suffixForGeneratedClass, evalCounter);

        // Generates code for super classes except java.lang.Object.
        Class<?> evaluatorClass = classLoader.loadClass(CodeGenHelper.toJdkStandardName(internalEvaluatorClassName));
        generateEvaluatorClassBinary(evaluatorClass.getSuperclass().getName(), suffixForGeneratedClass, evalCounter,
                nameMappings, classLoader, action);

        // Adds name mapping.
        nameMappings.add(Pair.of(internalEvaluatorClassName, targetEvaluatorClassName));
        nameMappings.add(Pair.of(CodeGenHelper.toJdkStandardName(internalEvaluatorClassName),
                CodeGenHelper.toJdkStandardName(targetEvaluatorClassName)));

        try (InputStream internalEvaluatorStream = getResourceStream(internalEvaluatorClassName, classLoader)) {
            ClassReader firstPassReader = new ClassReader(internalEvaluatorStream);
            // Generates inner classes other than the evaluator.
            Set<String> excludedNames = new HashSet<>();
            for (Pair<String, String> entry : nameMappings) {
                excludedNames.add(entry.getKey());
            }
            generateNonEvalInnerClasses(firstPassReader, excludedNames, nameMappings, suffixForGeneratedClass,
                    classLoader, action);

            // Injects missing-handling byte code.
            ClassWriter firstPassWriter = new ClassWriter(firstPassReader, 0);
            EvaluatorMissingCheckVisitor missingHandlingVisitor = new EvaluatorMissingCheckVisitor(firstPassWriter);
            firstPassReader.accept(missingHandlingVisitor, 0);

            ClassReader secondPassReader = new ClassReader(firstPassWriter.toByteArray());
            // Injects null-handling byte code and output the class binary.
            // Since we're going to add jump instructions, we have to let the ClassWriter to
            // automatically generate frames for JVM to verify the class.
            ClassWriter secondPassWriter =
                    new ClassWriter(secondPassReader, ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
            RenameClassVisitor renamingVisitor = new RenameClassVisitor(secondPassWriter, nameMappings);
            EvaluatorNullCheckVisitor nullHandlingVisitor =
                    new EvaluatorNullCheckVisitor(renamingVisitor, missingHandlingVisitor.getLastAddedLabel());
            secondPassReader.accept(nullHandlingVisitor, 0);
            action.runAction(targetEvaluatorClassName, secondPassWriter.toByteArray());
        }
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
            String targetInnerClassName = CodeGenHelper.generateClassName(innerClassName, suffix, counter++);
            nameMappings.add(Pair.of(innerClassName, targetInnerClassName));
            nameMappings.add(Pair.of(CodeGenHelper.toJdkStandardName(innerClassName),
                    CodeGenHelper.toJdkStandardName(targetInnerClassName)));

            // Renaming appearances of original class names.
            try (InputStream innerStream = getResourceStream(innerClassName, classLoader)) {
                ClassReader innerClassReader = new ClassReader(innerStream);
                ClassWriter writer = new ClassWriter(innerClassReader, 0);
                RenameClassVisitor renamingVisitor = new RenameClassVisitor(writer, nameMappings);
                innerClassReader.accept(renamingVisitor, 0);
                action.runAction(targetInnerClassName, writer.toByteArray());
            }
        }
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

    private CodeGenUtil() {
    }
}
