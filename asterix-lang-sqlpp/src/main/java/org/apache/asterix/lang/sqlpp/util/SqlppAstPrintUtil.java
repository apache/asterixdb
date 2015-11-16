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

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.sqlpp.visitor.SqlppPrintVisitor;

public class SqlppAstPrintUtil {

    /**
     * Prints the AST (abstract syntax tree) of an ILangExpression.
     * 
     * @param expr
     *            the language expression.
     * @param output
     *            a writer for printing strings.
     * @throws AsterixException
     */
    public static void print(ILangExpression expr, PrintWriter output) throws AsterixException {
        SqlppPrintVisitor visitor = new SqlppPrintVisitor(output);
        expr.accept(visitor, 0);
    }

    /**
     * Prints the AST of a list of top-level language statements.
     * 
     * @param statements
     *            a list of statements of a query
     * @param output
     *            a writer for printing strings.
     * @throws AsterixException
     */
    public static void print(List<Statement> statements, PrintWriter output) throws AsterixException {
        SqlppPrintVisitor visitor = new SqlppPrintVisitor(output);
        for (Statement statement : statements) {
            statement.accept(visitor, 0);
        }
    }

    /**
     * @param expr
     *            a language expression.
     * @return the AST of a language expression.
     * @throws AsterixException
     */
    public static String toString(ILangExpression expr) throws AsterixException {
        List<ILangExpression> exprs = new ArrayList<ILangExpression>();
        exprs.add(expr);
        return toString(exprs);
    }

    /**
     * @param exprs
     *            a list of language expression.
     * @return an AST of the input language expressions.
     * @throws AsterixException
     */
    public static String toString(List<ILangExpression> exprs) throws AsterixException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        PrintWriter output = new PrintWriter(bos);
        SqlppPrintVisitor visitor = new SqlppPrintVisitor(output);
        for (ILangExpression expr : exprs) {
            expr.accept(visitor, 0);
        }
        output.close();
        return new String(bos.toByteArray());
    }

}
