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
package org.apache.asterix.external.classad;

import java.util.HashMap;

import org.apache.asterix.external.classad.object.pool.ClassAdObjectPool;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableString;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FunctionCall extends ExprTree {

    public static boolean initialized = false;
    public static final HashMap<String, ClassAdFunc> funcTable = new HashMap<String, ClassAdFunc>();
    public static final ClassAdFunc[] ClassAdBuiltinFunc = { BuiltinClassAdFunctions.IsType,
            BuiltinClassAdFunctions.TestMember, BuiltinClassAdFunctions.Size, BuiltinClassAdFunctions.SumAvg,
            BuiltinClassAdFunctions.MinMax, BuiltinClassAdFunctions.ListCompare, BuiltinClassAdFunctions.debug,
            BuiltinClassAdFunctions.formatTime, BuiltinClassAdFunctions.getField, BuiltinClassAdFunctions.currentTime,
            BuiltinClassAdFunctions.timeZoneOffset, BuiltinClassAdFunctions.splitTime, BuiltinClassAdFunctions.dayTime,
            BuiltinClassAdFunctions.epochTime, BuiltinClassAdFunctions.strCat, BuiltinClassAdFunctions.changeCase,
            BuiltinClassAdFunctions.subString, BuiltinClassAdFunctions.convInt, BuiltinClassAdFunctions.compareString,
            BuiltinClassAdFunctions.matchPattern, BuiltinClassAdFunctions.matchPatternMember,
            BuiltinClassAdFunctions.substPattern, BuiltinClassAdFunctions.convReal, BuiltinClassAdFunctions.convString,
            BuiltinClassAdFunctions.unparse, BuiltinClassAdFunctions.convBool, BuiltinClassAdFunctions.convTime,
            BuiltinClassAdFunctions.doRound, BuiltinClassAdFunctions.doMath2, BuiltinClassAdFunctions.random,
            BuiltinClassAdFunctions.ifThenElse, BuiltinClassAdFunctions.stringListsIntersect,
            BuiltinClassAdFunctions.interval, BuiltinClassAdFunctions.eval };

    static {
        // load up the function dispatch table
        // type predicates
        funcTable.put("isundefined", BuiltinClassAdFunctions.IsType);
        funcTable.put("iserror", BuiltinClassAdFunctions.IsType);
        funcTable.put("isstring", BuiltinClassAdFunctions.IsType);
        funcTable.put("isinteger", BuiltinClassAdFunctions.IsType);
        funcTable.put("isreal", BuiltinClassAdFunctions.IsType);
        funcTable.put("islist", BuiltinClassAdFunctions.IsType);
        funcTable.put("isclassad", BuiltinClassAdFunctions.IsType);
        funcTable.put("isboolean", BuiltinClassAdFunctions.IsType);
        funcTable.put("isabstime", BuiltinClassAdFunctions.IsType);
        funcTable.put("isreltime", BuiltinClassAdFunctions.IsType);
        funcTable.put("isundefined", BuiltinClassAdFunctions.IsType);
        funcTable.put("isundefined", BuiltinClassAdFunctions.IsType);
        // list membership
        funcTable.put("member", BuiltinClassAdFunctions.TestMember);
        funcTable.put("identicalmember", BuiltinClassAdFunctions.TestMember);
        // Some list functions, useful for lists as sets
        funcTable.put("size", BuiltinClassAdFunctions.Size);
        funcTable.put("sum", BuiltinClassAdFunctions.SumAvg);
        funcTable.put("avg", BuiltinClassAdFunctions.SumAvg);
        funcTable.put("min", BuiltinClassAdFunctions.MinMax);
        funcTable.put("max", BuiltinClassAdFunctions.MinMax);
        funcTable.put("anycompare", BuiltinClassAdFunctions.ListCompare);
        funcTable.put("allcompare", BuiltinClassAdFunctions.ListCompare);
        //basic functions
        /*
        funcTable.put("sumfrom", BuiltinFunctions.SumAvgFrom);
        funcTable.put("avgfrom", BuiltinFunctions.SumAvgFrom);
        funcTable.put("maxfrom", BuiltinFunctions.BoundFrom);
        funcTable.put("minfrom", BuiltinFunctions.BoundFrom);
        */
        // time management
        funcTable.put("time", BuiltinClassAdFunctions.epochTime);
        funcTable.put("currenttime", BuiltinClassAdFunctions.currentTime);
        funcTable.put("timezoneoffset", BuiltinClassAdFunctions.timeZoneOffset);
        funcTable.put("daytime", BuiltinClassAdFunctions.dayTime);
        funcTable.put("getyear", BuiltinClassAdFunctions.getField);
        funcTable.put("getmonth", BuiltinClassAdFunctions.getField);
        funcTable.put("getdayofyear", BuiltinClassAdFunctions.getField);
        funcTable.put("getdayofmonth", BuiltinClassAdFunctions.getField);
        funcTable.put("getdayofweek", BuiltinClassAdFunctions.getField);
        funcTable.put("getdays", BuiltinClassAdFunctions.getField);
        funcTable.put("gethours", BuiltinClassAdFunctions.getField);
        funcTable.put("getminutes", BuiltinClassAdFunctions.getField);
        funcTable.put("getseconds", BuiltinClassAdFunctions.getField);
        funcTable.put("splittime", BuiltinClassAdFunctions.splitTime);
        funcTable.put("formattime", BuiltinClassAdFunctions.formatTime);
        // string manipulation
        funcTable.put("strcat", BuiltinClassAdFunctions.strCat);
        funcTable.put("toupper", BuiltinClassAdFunctions.changeCase);
        funcTable.put("tolower", BuiltinClassAdFunctions.changeCase);
        funcTable.put("substr", BuiltinClassAdFunctions.subString);
        funcTable.put("strcmp", BuiltinClassAdFunctions.compareString);
        funcTable.put("stricmp", BuiltinClassAdFunctions.compareString);
        // pattern matching (regular expressions)
        funcTable.put("regexp", BuiltinClassAdFunctions.matchPattern);
        funcTable.put("regexpmember", BuiltinClassAdFunctions.matchPatternMember);
        funcTable.put("regexps", BuiltinClassAdFunctions.substPattern);
        // conversion functions
        funcTable.put("int", BuiltinClassAdFunctions.convInt);
        funcTable.put("real", BuiltinClassAdFunctions.convReal);
        funcTable.put("string", BuiltinClassAdFunctions.convString);
        funcTable.put("bool", BuiltinClassAdFunctions.convBool);
        funcTable.put("abstime", BuiltinClassAdFunctions.convTime);
        funcTable.put("reltime", BuiltinClassAdFunctions.convTime);

        // turn the contents of an expression into a string
        // but *do not* evaluate it

        funcTable.put("unparse", BuiltinClassAdFunctions.unparse);
        // mathematical functions
        funcTable.put("floor", BuiltinClassAdFunctions.doRound);
        funcTable.put("ceil", BuiltinClassAdFunctions.doRound);
        funcTable.put("ceiling", BuiltinClassAdFunctions.doRound);
        funcTable.put("round", BuiltinClassAdFunctions.doRound);
        funcTable.put("pow", BuiltinClassAdFunctions.doMath2);
        funcTable.put("quantize", BuiltinClassAdFunctions.doMath2);
        funcTable.put("random", BuiltinClassAdFunctions.random);

        // for compatibility with old classads:
        funcTable.put("ifthenelse", BuiltinClassAdFunctions.ifThenElse);
        funcTable.put("interval", BuiltinClassAdFunctions.interval);
        funcTable.put("eval", BuiltinClassAdFunctions.eval);

        // string list functions:
        // Note that many other string list functions are defined
        // externally in the Condor classad compatibility layer.

        funcTable.put("stringlistsintersect", BuiltinClassAdFunctions.stringListsIntersect);
        funcTable.put("debug", BuiltinClassAdFunctions.debug);
        initialized = true;
    }

    // function call specific information
    private final CaseInsensitiveString functionName;
    private ClassAdFunc function;
    private final ExprList arguments;

    public FunctionCall(ClassAdObjectPool objectPool) {
        super(objectPool);
        functionName = new CaseInsensitiveString();
        arguments = new ExprList(objectPool);
        function = null;
    }

    public static FunctionCall createFunctionCall(String functionName, ExprList args, ClassAdObjectPool objectPool) {
        FunctionCall fc = objectPool != null ? objectPool.funcPool.get() : new FunctionCall(null);
        fc.function = funcTable.get(functionName.toLowerCase());
        fc.functionName.set(functionName);
        fc.arguments.setExprList(args.getExprList());
        return fc;
    }

    // start up with an argument list of size 4

    public FunctionCall(FunctionCall functioncall, ClassAdObjectPool objectPool) throws HyracksDataException {
        super(objectPool);
        functionName = new CaseInsensitiveString();
        arguments = new ExprList(objectPool);
        function = null;
        copyFrom(functioncall);
    }

    /**
     * Returns true if the function expression points to a valid
     * function in the ClassAd library.
     */
    public boolean functionIsDefined() {
        return function != null;
    }

    public void copyFrom(FunctionCall copiedFrom) throws HyracksDataException {
        this.function = copiedFrom.function;
        this.functionName.set(copiedFrom.functionName.get());
        this.arguments.setExprList(copiedFrom.arguments.getExprList());
    }

    @Override
    public ExprTree copy() throws HyracksDataException {
        FunctionCall newTree = objectPool.funcPool.get();
        newTree.copyFrom(this);
        return newTree;
    }

    @Override
    public void copyFrom(ExprTree tree) throws HyracksDataException {
        FunctionCall functioncall = (FunctionCall) tree;
        functionName.set(functioncall.functionName.get());
        function = functioncall.function;
        arguments.copyFrom(arguments);
        super.copyFrom(functioncall);
    }

    @Override
    public boolean sameAs(ExprTree tree) {
        boolean is_same = false;
        FunctionCall other_fn;
        ExprTree pSelfTree = tree.self();

        if (this == pSelfTree) {
            is_same = true;
        } else if (pSelfTree.getKind() != NodeKind.FN_CALL_NODE) {
            is_same = false;
        } else {
            try {
                other_fn = (FunctionCall) pSelfTree;
                if (functionName == other_fn.functionName && function.equals(other_fn.function)
                        && arguments.equals(other_fn.arguments)) {
                    is_same = true;

                } else {
                    is_same = false;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return is_same;
    }

    public boolean equals(FunctionCall fn) {
        return sameAs(fn);
    }

    public static HashMap<String, ClassAdFunc> getFunctionTable() {
        return funcTable;
    }

    public static synchronized void registerFunction(String functionName, ClassAdFunc function) {
        if (!funcTable.containsKey(functionName)) {
            funcTable.put(functionName, function);
        }
    }

    @Override
    public void privateSetParentScope(ClassAd parent) {
        arguments.privateSetParentScope(parent);
    }

    //This will move pointers to objects (not create clones)
    public void getComponents(AMutableString fn, ExprList exprList) {
        fn.setValue(functionName.get());
        for (ExprTree tree : arguments.getExprList()) {
            exprList.add(tree);
        }
    }

    public void getComponents(AMutableCharArrayString fn, ExprList exprList) {
        fn.setValue(functionName.get());
        for (ExprTree tree : arguments.getExprList()) {
            exprList.add(tree);
        }
    }

    @Override
    public boolean privateEvaluate(EvalState state, Value value) throws HyracksDataException {
        if (function != null) {
            return function.call(functionName.get(), arguments, state, value, objectPool);
        } else {
            value.setErrorValue();
            return (true);
        }
    }

    @Override
    public boolean privateEvaluate(EvalState state, Value value, ExprTreeHolder tree) throws HyracksDataException {
        FunctionCall tmpSig = objectPool.funcPool.get();
        Value tmpVal = objectPool.valuePool.get();
        ExprTreeHolder argSig = objectPool.mutableExprPool.get();
        MutableBoolean rval = objectPool.boolPool.get();
        if (!privateEvaluate(state, value)) {
            return false;
        }
        tmpSig.functionName.set(functionName.get());
        rval.setValue(true);
        for (ExprTree i : arguments.getExprList()) {
            rval.setValue(i.publicEvaluate(state, tmpVal, argSig));
            if (rval.booleanValue()) {
                tmpSig.arguments.add(argSig.getInnerTree());
            }
        }
        tree.setInnerTree(tmpSig);
        return rval.booleanValue();
    }

    @Override
    public boolean privateFlatten(EvalState state, Value value, ExprTreeHolder tree, AMutableInt32 i)
            throws HyracksDataException {
        FunctionCall newCall = objectPool.funcPool.get();
        ExprTreeHolder argTree = objectPool.mutableExprPool.get();
        Value argValue = objectPool.valuePool.get();
        boolean fold = true;
        tree.setInnerTree(null); // Just to be safe...  wenger 2003-12-11.

        // if the function cannot be resolved, the value is "error"
        if (function == null) {
            value.setErrorValue();
            return true;
        }

        newCall.functionName.set(functionName.get());
        newCall.function = function;

        // flatten the arguments
        for (ExprTree exp : arguments.getExprList()) {
            if (exp.publicFlatten(state, argValue, argTree)) {
                if (argTree.getInnerTree() != null) {
                    newCall.arguments.add(argTree.getInnerTree());
                    fold = false;
                    continue;
                } else {
                    // Assert: argTree == NULL
                    argTree.setInnerTree(Literal.createLiteral(argValue, objectPool));
                    if (argTree.getInnerTree() != null) {
                        newCall.arguments.add(argTree.getInnerTree());
                        continue;
                    }
                }
            }

            // we get here only when something bad happens
            value.setErrorValue();
            tree.setInnerTree(null);
            return false;
        }

        // assume all functions are "pure" (i.e., side-affect free)
        if (fold) {
            // flattened to a value
            if (!function.call(functionName.get(), arguments, state, value, objectPool)) {
                return false;
            }
            tree.setInnerTree(null);
        } else {
            tree.setInnerTree(newCall);
        }
        return true;
    }

    @Override
    public NodeKind getKind() {
        return NodeKind.FN_CALL_NODE;
    }

    @Override
    public void reset() {
        this.arguments.clear();
        this.function = null;
        this.functionName.set("");
    }
}
