/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hivesterix.logical.expression;

import java.util.HashMap;

import org.apache.hadoop.hive.ql.exec.Description;

import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class HiveAlgebricksBuiltInFunctionMap {

    /**
     * hive auqa builtin function map instance
     */
    public static HiveAlgebricksBuiltInFunctionMap INSTANCE = new HiveAlgebricksBuiltInFunctionMap();

    /**
     * hive to Algebricks function name mapping
     */
    private HashMap<String, FunctionIdentifier> hiveToAlgebricksMap = new HashMap<String, FunctionIdentifier>();

    /**
     * Algebricks to hive function name mapping
     */
    private HashMap<FunctionIdentifier, String> AlgebricksToHiveMap = new HashMap<FunctionIdentifier, String>();

    /**
     * the bi-directional mapping between hive functions and Algebricks
     * functions
     */
    private HiveAlgebricksBuiltInFunctionMap() {
        hiveToAlgebricksMap.put("and", AlgebricksBuiltinFunctions.AND);
        hiveToAlgebricksMap.put("or", AlgebricksBuiltinFunctions.OR);
        hiveToAlgebricksMap.put("!", AlgebricksBuiltinFunctions.NOT);
        hiveToAlgebricksMap.put("not", AlgebricksBuiltinFunctions.NOT);
        hiveToAlgebricksMap.put("=", AlgebricksBuiltinFunctions.EQ);
        hiveToAlgebricksMap.put("<>", AlgebricksBuiltinFunctions.NEQ);
        hiveToAlgebricksMap.put(">", AlgebricksBuiltinFunctions.GT);
        hiveToAlgebricksMap.put("<", AlgebricksBuiltinFunctions.LT);
        hiveToAlgebricksMap.put(">=", AlgebricksBuiltinFunctions.GE);
        hiveToAlgebricksMap.put("<=", AlgebricksBuiltinFunctions.LE);

        AlgebricksToHiveMap.put(AlgebricksBuiltinFunctions.AND, "and");
        AlgebricksToHiveMap.put(AlgebricksBuiltinFunctions.OR, "or");
        AlgebricksToHiveMap.put(AlgebricksBuiltinFunctions.NOT, "!");
        AlgebricksToHiveMap.put(AlgebricksBuiltinFunctions.NOT, "not");
        AlgebricksToHiveMap.put(AlgebricksBuiltinFunctions.EQ, "=");
        AlgebricksToHiveMap.put(AlgebricksBuiltinFunctions.NEQ, "<>");
        AlgebricksToHiveMap.put(AlgebricksBuiltinFunctions.GT, ">");
        AlgebricksToHiveMap.put(AlgebricksBuiltinFunctions.LT, "<");
        AlgebricksToHiveMap.put(AlgebricksBuiltinFunctions.GE, ">=");
        AlgebricksToHiveMap.put(AlgebricksBuiltinFunctions.LE, "<=");
    }

    /**
     * get hive function name from Algebricks function identifier
     * 
     * @param AlgebricksId
     * @return hive
     */
    public String getHiveFunctionName(FunctionIdentifier AlgebricksId) {
        return AlgebricksToHiveMap.get(AlgebricksId);
    }

    /**
     * get hive UDF or Generic class's corresponding built-in functions
     * 
     * @param funcClass
     * @return function identifier
     */
    public FunctionIdentifier getAlgebricksFunctionId(Class<?> funcClass) {
        Description annotation = (Description) funcClass.getAnnotation(Description.class);
        String hiveUDFName = "";
        if (annotation == null) {
            hiveUDFName = null;
            return null;
        } else {
            hiveUDFName = annotation.name();
            return hiveToAlgebricksMap.get(hiveUDFName);
        }
    }
}
