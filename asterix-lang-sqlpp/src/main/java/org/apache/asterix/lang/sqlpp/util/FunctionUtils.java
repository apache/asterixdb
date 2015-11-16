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

package org.apache.asterix.lang.sqlpp.util;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.parser.SQLPPParser;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

public class FunctionUtils {

    public static final String IMPORT_PRIVATE_FUNCTIONS = "import-private-functions";

    public static FunctionDecl getFunctionDecl(Function function) throws AsterixException {
        String functionBody = function.getFunctionBody();
        List<String> params = function.getParams();
        List<VarIdentifier> varIdentifiers = new ArrayList<VarIdentifier>();

        StringBuilder builder = new StringBuilder();
        builder.append(" use dataverse " + function.getDataverseName() + ";");
        builder.append(" declare function " + function.getName().split("@")[0]);
        builder.append("(");
        for (String param : params) {
            VarIdentifier varId = new VarIdentifier(param);
            varIdentifiers.add(varId);
            builder.append(param);
            builder.append(",");
        }
        if (params.size() > 0) {
            builder.delete(builder.length() - 1, builder.length());
        }
        builder.append(")");
        builder.append("{");
        builder.append("\n");
        builder.append(functionBody);
        builder.append("\n");
        builder.append("}");

        SQLPPParser parser = new SQLPPParser(new StringReader(new String(builder)));

        List<Statement> statements = null;
        try {
            statements = parser.parse();
        } catch (AsterixException pe) {
            throw new AsterixException(pe);
        }

        FunctionDecl decl = (FunctionDecl) statements.get(1);
        return decl;
    }

    public static IFunctionInfo getFunctionInfo(FunctionIdentifier fi) {
        return AsterixBuiltinFunctions.getAsterixFunctionInfo(fi);
    }

    public static IFunctionInfo getFunctionInfo(FunctionSignature fs) {
        return getFunctionInfo(new FunctionIdentifier(fs.getNamespace(), fs.getName(), fs.getArity()));
    }

}
