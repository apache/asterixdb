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
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * This visitor gathers all created evaluators in an evaluator factory.
 */
public class GatherEvaluatorCreationVisitor extends ClassVisitor {

    private static String METHOD_NAME = "createScalarEvaluator";
    private Set<String> createdEvaluatorClassNames = new HashSet<>();
    private String ownerPrefix;

    public GatherEvaluatorCreationVisitor(String ownerPrefix) {
        super(Opcodes.ASM5);
        this.ownerPrefix = ownerPrefix;
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        if (!name.equals(METHOD_NAME)) {
            return null;
        }
        return new MethodVisitor(Opcodes.ASM5, null) {

            @Override
            public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {
                if (opcode != Opcodes.INVOKESPECIAL) {
                    return;
                }
                if (owner.startsWith(ownerPrefix)) {
                    createdEvaluatorClassNames.add(owner);
                }
            }
        };

    }

    public Set<String> getCreatedEvaluatorClassNames() {
        return createdEvaluatorClassNames;
    }

}
