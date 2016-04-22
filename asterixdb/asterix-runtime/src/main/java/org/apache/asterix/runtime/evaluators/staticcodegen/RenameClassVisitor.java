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

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * This visitor replaces all the appearances of original class names with
 * new (generated) class names, according to an input name mapping.
 */
public class RenameClassVisitor extends ClassVisitor {

    private final List<Pair<String, String>> nameMapping;

    public RenameClassVisitor(ClassVisitor downStreamVisitor, List<Pair<String, String>> nameMapping) {
        super(Opcodes.ASM5, downStreamVisitor);
        this.nameMapping = nameMapping;
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        super.visit(version, access, applyMapping(name), signature, applyMapping(superName), interfaces);
    }

    @Override
    public void visitOuterClass(String owner, String name, String desc) {
        // Skips outer class descriptions.
    }

    @Override
    public void visitInnerClass(String name, String outerName, String innerName, int access) {
        // Skips inner class descriptions.
    }

    @Override
    public FieldVisitor visitField(int access, String name, String desc, String signature, Object value) {
        return cv.visitField(access, name, applyMapping(desc), applyMapping(signature), value);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        MethodVisitor mv = cv.visitMethod(access, name, applyMapping(desc), applyMapping(signature), exceptions);
        if (mv != null) {
            return new MethodVisitor(Opcodes.ASM5, mv) {

                @Override
                public void visitFieldInsn(int opcode, String owner, String name, String desc) {
                    mv.visitFieldInsn(opcode, applyMapping(owner), applyMapping(name), applyMapping(desc));
                }

                @Override
                public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {
                    mv.visitMethodInsn(opcode, applyMapping(owner), name, applyMapping(desc), itf);
                }

                @Override
                public void visitLocalVariable(String name, String desc, String signature, Label start, Label end,
                        int index) {
                    mv.visitLocalVariable(name, applyMapping(desc), applyMapping(signature), start, end, index);
                }

                @Override
                public void visitFrame(int type, int nLocal, Object[] local, int nStack, Object[] stack) {
                    if (local != null) {
                        for (int index = 0; index < local.length; ++index) {
                            if (local[index] instanceof String) {
                                local[index] = applyMapping((String) local[index]);
                            }
                        }
                    }
                    mv.visitFrame(type, nLocal, local, nStack, stack);
                }

                @Override
                public void visitTypeInsn(int opcode, String type) {
                    mv.visitTypeInsn(opcode, applyMapping(type));
                }

            };
        }
        return null;
    }

    private String applyMapping(String inputStr) {
        return CodeGenUtil.applyMapping(nameMapping, inputStr);
    }

}
