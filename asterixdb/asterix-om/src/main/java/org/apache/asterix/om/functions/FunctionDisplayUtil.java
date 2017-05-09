/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.om.functions;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

class FunctionDisplayUtil {

    @FunctionalInterface
    interface DefaultDisplayFunction {
        String display(List<Mutable<ILogicalExpression>> args);
    }

    private FunctionDisplayUtil() {
        // Does nothing.
    }

    /**
     * Displays a function with its parameters in a user-friendly way.
     *
     * @param functionInfo,
     *            the function info.
     * @param args
     *            , the arguments in the function call expression.
     * @param defaultDisplayFunction,
     *            the default display function for regular functions.
     * @return the display string of the function call expression.
     */
    public static String display(IFunctionInfo functionInfo, List<Mutable<ILogicalExpression>> args,
            DefaultDisplayFunction defaultDisplayFunction) {
        FunctionIdentifier funcId = functionInfo.getFunctionIdentifier();
        if (funcId.equals(BuiltinFunctions.FIELD_ACCESS_BY_INDEX)) {
            return displayFieldAccess(args, true);
        } else if (funcId.equals(BuiltinFunctions.FIELD_ACCESS_BY_NAME)
                || funcId.equals(BuiltinFunctions.FIELD_ACCESS_NESTED)) {
            return displayFieldAccess(args, false);
        } else if (funcId.equals(BuiltinFunctions.OPEN_RECORD_CONSTRUCTOR)
                || funcId.equals(BuiltinFunctions.CLOSED_RECORD_CONSTRUCTOR)) {
            return displayRecordConstructor(args);
        }
        return defaultDisplayFunction.display(args);
    }

    // Displays field-access-by-index in an easy-to-understand way.
    private static String displayFieldAccess(List<Mutable<ILogicalExpression>> args, boolean intArg) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        boolean second = true;
        for (Mutable<ILogicalExpression> ref : args) {
            if (first) {
                first = false;
            } else if (second) {
                sb.append(".getField(" + (intArg ? "" : "\""));
                second = false;
            } else {
                sb.append(".");
            }
            sb.append(ref.getValue().toString().replaceAll("^\"|\"$", ""));
        }
        sb.append((intArg ? "" : "\"") + ")");
        return sb.toString();
    }

    // Displays record-constructor in an easy-to-understand way.
    private static String displayRecordConstructor(List<Mutable<ILogicalExpression>> args) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        boolean fieldName = true;
        sb.append("{");
        for (Mutable<ILogicalExpression> ref : args) {
            if (first) {
                first = false;
            } else if (fieldName) {
                sb.append(", ");
            }
            sb.append(ref.getValue());
            if (fieldName) {
                sb.append(": ");
                fieldName = false;
            } else {
                fieldName = true;
            }
        }
        sb.append("}");
        return sb.toString();
    }

}
