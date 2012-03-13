/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.algebricks.core.algebra.functions;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class AlgebricksBuiltinFunctions {
    public enum ComparisonKind {
        EQ,
        LE,
        GE,
        LT,
        GT,
        NEQ
    }

    private static Map<FunctionIdentifier, FunctionIdentifier> algebricksBuiltinFunctions = new HashMap<FunctionIdentifier, FunctionIdentifier>();

    private static Map<FunctionIdentifier, IFunctionInfo> _finfos = new HashMap<FunctionIdentifier, IFunctionInfo>();

    public static final String ALGEBRICKS_NS = "algebricks";

    // comparisons
    public final static FunctionIdentifier EQ = new FunctionIdentifier(ALGEBRICKS_NS, "eq", 2, true);
    public final static FunctionIdentifier LE = new FunctionIdentifier(ALGEBRICKS_NS, "le", 2, true);
    public final static FunctionIdentifier GE = new FunctionIdentifier(ALGEBRICKS_NS, "ge", 2, true);
    public final static FunctionIdentifier LT = new FunctionIdentifier(ALGEBRICKS_NS, "lt", 2, true);
    public final static FunctionIdentifier GT = new FunctionIdentifier(ALGEBRICKS_NS, "gt", 2, true);
    public final static FunctionIdentifier NEQ = new FunctionIdentifier(ALGEBRICKS_NS, "neq", 2, true);

    // booleans
    public final static FunctionIdentifier NOT = new FunctionIdentifier(ALGEBRICKS_NS, "not", 1, true);
    public final static FunctionIdentifier AND = new FunctionIdentifier(ALGEBRICKS_NS, "and",
            FunctionIdentifier.VARARGS, true);
    public final static FunctionIdentifier OR = new FunctionIdentifier(ALGEBRICKS_NS, "or", FunctionIdentifier.VARARGS,
            true);

    // numerics
    public final static FunctionIdentifier NUMERIC_ADD = new FunctionIdentifier(ALGEBRICKS_NS, "numeric-add", 2, true);

    // nulls
    public final static FunctionIdentifier IS_NULL = new FunctionIdentifier(ALGEBRICKS_NS, "is-null", 1, true);

    static {
        // comparisons
        add(EQ);
        add(LE);
        add(GE);
        add(LT);
        add(GT);
        add(NEQ);
        // booleans
        add(NOT);
        add(AND);
        add(OR);
        // numerics
        add(NUMERIC_ADD);
        // nulls
        add(IS_NULL);
        for (FunctionIdentifier fi : algebricksBuiltinFunctions.values()) {
            _finfos.put(fi, new FunctionInfoImpl(fi));
        }
    }

    private static void add(FunctionIdentifier fi) {
        algebricksBuiltinFunctions.put(fi, fi);
    }

    public static final boolean isAlgebricksBuiltinFunction(FunctionIdentifier fi) {
        return algebricksBuiltinFunctions.get(fi) != null;
    }

    public static final Collection<FunctionIdentifier> getAlgebricksBuiltinFunctions() {
        return algebricksBuiltinFunctions.values();
    }

    public static final FunctionIdentifier getBuiltinFunctionIdentifier(FunctionIdentifier fi) {
        return algebricksBuiltinFunctions.get(fi);
    }

    private static final Map<FunctionIdentifier, ComparisonKind> comparisonFunctions = new HashMap<FunctionIdentifier, ComparisonKind>();
    static {
        comparisonFunctions.put(AlgebricksBuiltinFunctions.EQ, ComparisonKind.EQ);
        comparisonFunctions.put(AlgebricksBuiltinFunctions.LE, ComparisonKind.LE);
        comparisonFunctions.put(AlgebricksBuiltinFunctions.GE, ComparisonKind.GE);
        comparisonFunctions.put(AlgebricksBuiltinFunctions.LT, ComparisonKind.LT);
        comparisonFunctions.put(AlgebricksBuiltinFunctions.GT, ComparisonKind.GT);
        comparisonFunctions.put(AlgebricksBuiltinFunctions.NEQ, ComparisonKind.NEQ);
    }

    public static ComparisonKind getComparisonType(FunctionIdentifier fi) {
        return comparisonFunctions.get(fi);
    }

    public static boolean isComparisonFunction(FunctionIdentifier fi) {
        return comparisonFunctions.get(fi) != null;
    }

    public static IFunctionInfo getBuiltinFunctionInfo(FunctionIdentifier fi) {
        return _finfos.get(fi);
    }

}
