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

package org.apache.asterix.common.utils;

public final class CodeGenHelper {

    public final static String DEFAULT_SUFFIX_FOR_GENERATED_CLASS = "Gen";

    private final static String DOLLAR = "$";

    private final static String NESTED_CLASSNAME_PREFIX = "_";

    public static String getGeneratedClassName(String originalClassName, String suffixForGeneratedClass) {
        return toJdkStandardName(getGeneratedInternalClassName(originalClassName, suffixForGeneratedClass));
    }

    public static String getGeneratedInternalClassName(String originalClassName, String suffixForGeneratedClass) {
        String originalFuncDescriptorClassInternalName = toInternalClassName(originalClassName);
        return generateClassName(originalFuncDescriptorClassInternalName, suffixForGeneratedClass, 0);
    }

    /**
     * Gets the name of a generated class.
     *
     * @param originalClassName,
     *            the original class, i.e., the source of the generated class.
     * @param suffix,
     *            the suffix for the generated class.
     * @param counter,
     *            a counter that appearing at the end of the name of the generated class.
     * @return the name of the generated class.
     */
    public static String generateClassName(String originalClassName, String suffix, int counter) {
        StringBuilder sb = new StringBuilder();
        int end = originalClassName.indexOf(DOLLAR);
        if (end < 0) {
            end = originalClassName.length();
        }

        String name = originalClassName.substring(0, end);
        sb.append(name);
        sb.append(DOLLAR);
        sb.append(NESTED_CLASSNAME_PREFIX);
        sb.append(suffix);

        if (counter > 0) {
            sb.append(counter);
        }
        return sb.toString();
    }

    /**
     * Converts an ASM class name to the JDK class naming format.
     *
     * @param name,
     *            a class name following the ASM convention.
     * @return a "."-separated class name for JDK.
     */
    public static String toJdkStandardName(String name) {
        return name.replace("/", ".");
    }

    /**
     * Converts a JDK class name to the class naming format of ASM.
     *
     * @param name,
     *            a class name following the JDK convention.
     * @return a "/"-separated class name assumed by ASM.
     */
    public static String toInternalClassName(String name) {
        return name.replace(".", "/");
    }

    private CodeGenHelper() {
    }
}
