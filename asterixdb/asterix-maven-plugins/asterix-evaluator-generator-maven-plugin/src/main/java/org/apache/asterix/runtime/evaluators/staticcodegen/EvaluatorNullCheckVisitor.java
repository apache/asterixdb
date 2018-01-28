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

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * This visitor adds null-handling byte code into an evaluator class.
 */
public class EvaluatorNullCheckVisitor extends ClassVisitor {
    private final static String EVALUATE_DESC = "(Lorg/apache/hyracks/dataflow/common/data/accessors/"
            + "IFrameTupleReference;Lorg/apache/hyracks/data/std/api/IPointable;)V";
    private final static String EVALUATE = "evaluate";
    private final static MethodIdentifier METHOD_IDENTIFIER = new MethodIdentifier(EVALUATE, EVALUATE_DESC, null);
    private final static String TYPE_CHECKER_CLASS =
            "org/apache/asterix/runtime/evaluators/staticcodegen/" + "TypeChecker";
    private final static String TYPE_CHECKER_DESC = "L" + TYPE_CHECKER_CLASS + ";";
    private final static String TYPE_CHECKER_NAME = "typeChecker";
    private final static String IS_NULL = "isNull";
    private final static String TYPECHECK_METHOD_DESC = "(Lorg/apache/hyracks/data/std/api/IPointable;)Z";
    private String className = null;
    private final Label lastAddedLabel;

    public EvaluatorNullCheckVisitor(ClassVisitor downStreamVisitor, Label lastAddedLabel) {
        super(Opcodes.ASM5, downStreamVisitor);
        this.lastAddedLabel = lastAddedLabel;
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        if (cv != null) {
            cv.visit(version, access, name, signature, superName, interfaces);
        }
        this.className = name;
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        MethodVisitor mv = cv.visitMethod(access, name, desc, signature, exceptions);
        if (!METHOD_IDENTIFIER.equals(new MethodIdentifier(name, desc, signature))) {
            return mv;
        }
        if (mv != null) {
            return new InjectNullCheckVisitor(Opcodes.ASM5, mv);
        }
        return null;
    }

    // Obtains the last added label.
    Label getLastAddedLabel() {
        return lastAddedLabel;
    }

    class InjectNullCheckVisitor extends MethodVisitor {

        public InjectNullCheckVisitor(int api, MethodVisitor mv) {
            super(api, mv);
        }

        @Override
        public void visitLabel(Label label) {
            // Emits the label.
            mv.visitLabel(label);

            // Injects null-handling after the last missing-handling byte code.
            if (lastAddedLabel == null || lastAddedLabel.getOffset() != label.getOffset()) {
                return;
            }

            // Loads the callee.
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitFieldInsn(Opcodes.GETFIELD, className, TYPE_CHECKER_NAME, TYPE_CHECKER_DESC);

            // Loads the result IPointable.
            mv.visitVarInsn(Opcodes.ALOAD, 2);

            // Invokes the null check method.
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, TYPE_CHECKER_CLASS, IS_NULL, TYPECHECK_METHOD_DESC, false);
            Label notNull = new Label();
            // Adds the if branch.
            mv.visitJumpInsn(Opcodes.IFEQ, notNull);
            mv.visitInsn(Opcodes.RETURN);
            mv.visitLabel(notNull);
        }
    }
}
