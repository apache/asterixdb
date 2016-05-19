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

import java.util.HashSet;
import java.util.Set;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Opcodes;

/**
 * This class gathers all inner classes defined in a class.
 */
public class GatherInnerClassVisitor extends ClassVisitor {

    private final Set<String> innerClassNames = new HashSet<>();
    private String className = null;

    public GatherInnerClassVisitor() {
        super(Opcodes.ASM5);
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        className = name;
    }

    @Override
    public void visitInnerClass(String name, String outerName, String innerName, int access) {
        if (className == null || !name.equals(className)) {
            innerClassNames.add(name);
        }
    }

    public Set<String> getInnerClassNames() {
        return innerClassNames;
    }
}
