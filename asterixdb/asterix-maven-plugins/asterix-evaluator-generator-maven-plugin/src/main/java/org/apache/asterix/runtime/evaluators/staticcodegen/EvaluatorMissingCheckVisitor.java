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
import org.objectweb.asm.tree.VarInsnNode;

/**
 * This visitor adds missing-handling byte code into an evaluator class.
 */
public class EvaluatorMissingCheckVisitor extends ClassVisitor {
    private static final String EVALUATE_DESC = "(Lorg/apache/hyracks/dataflow/common/data/"
            + "accessors/IFrameTupleReference;Lorg/apache/hyracks/data/std/api/IPointable;)V";
    private static final String EVALUATE = "evaluate";
    private static final MethodIdentifier METHOD_IDENTIFIER = new MethodIdentifier(EVALUATE, EVALUATE_DESC, null);
    private static final String TYPE_CHECKER_CLASS = "org/apache/asterix/runtime/evaluators/staticcodegen/TypeChecker";
    private static final String TYPE_CHECKER_DESC = "L" + TYPE_CHECKER_CLASS + ";";
    private static final String TYPE_CHECKER_NAME = "typeChecker";
    private static final String IS_MISSING = "isMissing";
    private static final String TYPECHECK_METHOD_DESC =
            "(Lorg/apache/hyracks/data/std/api/IPointable;" + "Lorg/apache/hyracks/data/std/api/IPointable;)Z";
    private static final String CONSTRUCTOR = "<init>";
    private String className = null;
    private Label lastAddedLabel = null;

    public EvaluatorMissingCheckVisitor(ClassVisitor downStreamVisitor) {
        super(Opcodes.ASM5, downStreamVisitor);
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        if (cv != null) {
            cv.visit(version, access, name, signature, superName, interfaces);
        }
        this.className = name;
    }

    @Override
    public void visitEnd() {
        if (cv != null) {
            cv.visitField(Opcodes.ACC_PRIVATE | Opcodes.ACC_FINAL, TYPE_CHECKER_NAME, TYPE_CHECKER_DESC, null, null);
            cv.visitEnd();
        }
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        MethodVisitor mv = cv.visitMethod(access, name, desc, signature, exceptions);
        if (!METHOD_IDENTIFIER.equals(new MethodIdentifier(name, desc, signature)) && !name.equals(CONSTRUCTOR)) {
            return mv;
        }
        if (name.equals(CONSTRUCTOR) && mv != null) {
            return new ConstructorVisitor(Opcodes.ASM5, mv);
        }
        if (mv != null) {
            return new InjectMissingCheckVisitor(Opcodes.ASM5, mv);
        }
        return null;
    }

    // Obtains the last added label.
    Label getLastAddedLabel() {
        return lastAddedLabel;
    }

    class ConstructorVisitor extends MethodVisitor {

        public ConstructorVisitor(int api, MethodVisitor mv) {
            super(api, mv);
        }

        @Override
        public void visitInsn(int opcode) {
            if (opcode != Opcodes.RETURN) {
                mv.visitInsn(opcode);
                return;
            }
            // Loads "this".
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            // New TypeChecker.
            mv.visitTypeInsn(Opcodes.NEW, TYPE_CHECKER_CLASS);
            // Duplicate the top operand.
            mv.visitInsn(Opcodes.DUP);
            // Invoke the constructor of TypeChecker.
            mv.visitMethodInsn(Opcodes.INVOKESPECIAL, TYPE_CHECKER_CLASS, CONSTRUCTOR, "()V", false);
            // Putfield for the field typeChecker.
            mv.visitFieldInsn(Opcodes.PUTFIELD, className, TYPE_CHECKER_NAME, TYPE_CHECKER_DESC);
            // RETURN.
            mv.visitInsn(Opcodes.RETURN);
        }
    }

    class InjectMissingCheckVisitor extends MethodVisitor {

        private FieldInsnNode fieldAccessNode = null;
        private List<AbstractInsnNode> instructionsAfterFieldAccess = new ArrayList<>();
        private boolean updateToNextLabel = false;

        public InjectMissingCheckVisitor(int opcode, MethodVisitor mv) {
            super(opcode, mv);
        }

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
        public void visitVarInsn(int opcode, int operand) {
            if (fieldAccessNode != null) {
                instructionsAfterFieldAccess.add(new VarInsnNode(opcode, operand));
            }
            super.visitVarInsn(opcode, operand);
        }

        @Override
        public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {
            mv.visitMethodInsn(opcode, owner, name, desc, itf);
            if (fieldAccessNode == null || !METHOD_IDENTIFIER.equals(new MethodIdentifier(name, desc, null))) {
                return;
            }

            // Loads the callee.
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitFieldInsn(Opcodes.GETFIELD, className, TYPE_CHECKER_NAME, TYPE_CHECKER_DESC);

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

            // Invokes the missing check method.
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, TYPE_CHECKER_CLASS, IS_MISSING, TYPECHECK_METHOD_DESC, false);
            lastAddedLabel = new Label();
            // Adds the if branch.
            mv.visitJumpInsn(Opcodes.IFEQ, lastAddedLabel);
            mv.visitInsn(Opcodes.RETURN);
            mv.visitLabel(lastAddedLabel);
        }

        @Override
        public void visitLabel(Label label) {
            if (updateToNextLabel) {
                lastAddedLabel = label;
                updateToNextLabel = false;
            }
            super.visitLabel(label);
        }

        @Override
        public void visitJumpInsn(int opcode, Label label) {
            super.visitJumpInsn(opcode, label);
            if (lastAddedLabel == null) {
                return;
            }
            try {
                if (label.getOffset() < lastAddedLabel.getOffset()) {
                    // Backward jump, i.e., loop.
                    updateToNextLabel = true;
                }
            } catch (IllegalStateException e) {
                // Forward jump, the offset is not available.
            }
        }
    }

}
