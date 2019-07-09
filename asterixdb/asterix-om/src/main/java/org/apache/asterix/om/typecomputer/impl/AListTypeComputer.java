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

package org.apache.asterix.om.typecomputer.impl;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.typecomputer.base.AbstractResultTypeComputer;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;

/**
 * Returns a list that is missable/nullable. The list type is taken from one of the input args which is the input list.
 */
public class AListTypeComputer extends AbstractResultTypeComputer {
    public static final AListTypeComputer INSTANCE_REMOVE = new AListTypeComputer(2, false, false, true);
    public static final AListTypeComputer INSTANCE_PUT = new AListTypeComputer(2, false, true, true);
    public static final AListTypeComputer INSTANCE_PREPEND = new AListTypeComputer(2, true, true, false);
    public static final AListTypeComputer INSTANCE_APPEND = new AListTypeComputer(2, false, true, false);
    public static final AListTypeComputer INSTANCE_INSERT = new AListTypeComputer(3, false, true, false);
    public static final AListTypeComputer INSTANCE_REPLACE = new AListTypeComputer(3, false, true, false);
    public static final AListTypeComputer INSTANCE_SLICE = new AListTypeComputer(-1, false, false, true);

    private final int minNumArgs;
    private final boolean listIsLast;
    private final boolean makeOpen;
    private final boolean nullInNullOut;

    // Use this constructor to skip checking the arguments count
    private AListTypeComputer(int minNumArgs, boolean listIsLast, boolean makeOpen, boolean nullInNullOut) {
        this.minNumArgs = minNumArgs;
        this.listIsLast = listIsLast;
        this.makeOpen = makeOpen;
        this.nullInNullOut = nullInNullOut;
    }

    @Override
    protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) throws AlgebricksException {
        if (minNumArgs > -1 && strippedInputTypes.length < minNumArgs) {
            String functionName = ((AbstractFunctionCallExpression) expr).getFunctionIdentifier().getName();
            throw new CompilationException(ErrorCode.COMPILATION_INVALID_NUM_OF_ARGS, expr.getSourceLocation(),
                    functionName);
        }
        // output type should be the same as as the type tag at [list index]. The output type is nullable/missable
        // since the output could be null due to other invalid arguments or the tag at [list index] itself is not list
        int listIndex = 0;
        if (listIsLast) {
            listIndex = strippedInputTypes.length - 1;
        }

        IAType listType = strippedInputTypes[listIndex];
        if (listType.getTypeTag().isListType()) {
            if (makeOpen) {
                listType = DefaultOpenFieldType.getDefaultOpenFieldType(listType.getTypeTag());
            }
            return AUnionType.createUnknownableType(listType);
        } else {
            return BuiltinType.ANY;
        }
    }

    @Override
    protected boolean propagateNullAndMissing() {
        return nullInNullOut;
    }
}
