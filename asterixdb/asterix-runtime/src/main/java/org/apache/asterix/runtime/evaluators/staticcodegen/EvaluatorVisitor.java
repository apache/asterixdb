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

import java.util.ArrayList;
import java.util.List;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.FieldInsnNode;
import org.objectweb.asm.tree.IincInsnNode;
import org.objectweb.asm.tree.InsnNode;
import org.objectweb.asm.tree.IntInsnNode;

/**
 * This visitor adds null-handling byte code into an evaluator class.
 */
public class EvaluatorVisitor extends ClassVisitor {
    private final static String EVALUATE_DESC = "(Lorg/apache/hyracks/dataflow/common/data/accessors/IFrameTupleReference;Lorg/apache/hyracks/data/std/api/IPointable;)V";
    private final static String EVALUATE = "evaluate";
    private final static MethodIdentifier METHOD_IDENTIFIER = new MethodIdentifier(EVALUATE, EVALUATE_DESC, null);
    private final static String TYPECHECK_CLASS = "org/apache/asterix/runtime/evaluators/staticcodegen/TypeCheckUtil";
    private final static String IS_NULL = "isNull";
    private final static String TYPECHECK_METHOD_DESC = "(Lorg/apache/hyracks/data/std/api/IPointable;Lorg/apache/hyracks/data/std/api/IPointable;)Z";

    public EvaluatorVisitor(ClassVisitor downStreamVisitor) {
        super(Opcodes.ASM5, downStreamVisitor);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        MethodVisitor mv = cv.visitMethod(access, name, desc, signature, exceptions);
        if (!METHOD_IDENTIFIER.equals(new MethodIdentifier(name, desc, signature))) {
            return mv;
        }
        if (mv != null) {
            return new MethodVisitor(Opcodes.ASM5, mv) {
                private FieldInsnNode fieldAccessNode = null;
                private List<AbstractInsnNode> instructionsAfterFieldAccess = new ArrayList<>();

                @Override
                public void visitFieldInsn(int opcode, String owner, String name, String desc) {
                    mv.visitFieldInsn(opcode, owner, name, desc);
                    fieldAccessNode = new FieldInsnNode(opcode, owner, name, desc);
                    instructionsAfterFieldAccess.clear();
                }

                @Override
                public void visitIincInsn(int var, int increment) {
                    if (fieldAccessNode != null) {
                        instructionsAfterFieldAccess.add(new IincInsnNode(var, increment));
                    }
                    super.visitIincInsn(var, increment);
                }

                @Override
                public void visitInsn(int opcode) {
                    if (fieldAccessNode != null) {
                        instructionsAfterFieldAccess.add(new InsnNode(opcode));
                    }
                    super.visitInsn(opcode);
                }

                @Override
                public void visitIntInsn(int opcode, int operand) {
                    if (fieldAccessNode != null) {
                        instructionsAfterFieldAccess.add(new IntInsnNode(opcode, operand));
                    }
                    super.visitIntInsn(opcode, operand);
                }

                @Override
                public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {
                    mv.visitMethodInsn(opcode, owner, name, desc, itf);
                    if (fieldAccessNode == null
                            || !METHOD_IDENTIFIER.equals(new MethodIdentifier(name, desc, signature))) {
                        return;
                    }
                    // Loads "this".
                    mv.visitVarInsn(Opcodes.ALOAD, 0);
                    // Replays the field access instruction.
                    fieldAccessNode.accept(mv);

                    // Replays other instruction between the field access and the evaluator call.
                    for (AbstractInsnNode instruction : instructionsAfterFieldAccess) {
                        instruction.accept(mv);
                    }

                    // Loads the result IPointable.
                    mv.visitVarInsn(Opcodes.ALOAD, 2);

                    // Invokes the null check method.
                    mv.visitMethodInsn(Opcodes.INVOKESTATIC, TYPECHECK_CLASS, IS_NULL, TYPECHECK_METHOD_DESC, false);
                    Label notNull = new Label();
                    // Adds the if branch.
                    mv.visitJumpInsn(Opcodes.IFEQ, notNull);
                    mv.visitInsn(Opcodes.RETURN);
                    mv.visitLabel(notNull);
                    mv.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
                }
            };
        }
        return null;
    }

}
