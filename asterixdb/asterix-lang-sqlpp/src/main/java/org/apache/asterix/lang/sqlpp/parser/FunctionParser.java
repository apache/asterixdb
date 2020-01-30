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

package org.apache.asterix.lang.sqlpp.parser;

import java.io.StringReader;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.DataverseNameUtils;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class FunctionParser {

    private final IParserFactory parserFactory;

    public FunctionParser(IParserFactory parserFactory) {
        this.parserFactory = parserFactory;
    }

    public FunctionDecl getFunctionDecl(Function function) throws CompilationException {
        if (!function.getLanguage().equals(Function.LANGUAGE_SQLPP)) {
            throw new CompilationException(ErrorCode.COMPILATION_INCOMPATIBLE_FUNCTION_LANGUAGE,
                    Function.LANGUAGE_SQLPP, function.getLanguage());
        }

        String functionBody = function.getFunctionBody();
        List<String> argNames = function.getArgNames();
        List<Pair<DataverseName, IAType>> args = function.getArguments();

        StringBuilder builder = new StringBuilder();
        builder.append(" use " + DataverseNameUtils.generateDataverseName(function.getDataverseName()) + ";");
        builder.append(" declare function " + function.getName().split("@")[0]);
        builder.append("(");
        for (int i = 0; i < argNames.size(); i++) {
            String param = argNames.get(i);
            String type = null;
            if (args.get(i) != null) {
                Pair<DataverseName, IAType> t = args.get(i);
                String argToStringType = t.getFirst().getCanonicalForm() + "." + t.getSecond().getTypeName();
                if (!"asterix.any".equalsIgnoreCase(argToStringType)) {
                    type = argToStringType;
                }
            }
            VarIdentifier varId = SqlppVariableUtil.toUserDefinedVariableName(param);
            builder.append(varId);
            if (type != null) {
                builder.append(":");
                builder.append(type);
            }
            builder.append(",");
        }
        if (argNames.size() > 0) {
            builder.delete(builder.length() - 1, builder.length());
        }
        builder.append(")");
        builder.append("{");
        builder.append("\n");
        builder.append(functionBody);
        builder.append("\n");
        builder.append("};");

        IParser parser = parserFactory.createParser(new StringReader(new String(builder)));
        List<Statement> statements = parser.parse();
        FunctionDecl decl = (FunctionDecl) statements.get(1);
        return decl;
    }

}
