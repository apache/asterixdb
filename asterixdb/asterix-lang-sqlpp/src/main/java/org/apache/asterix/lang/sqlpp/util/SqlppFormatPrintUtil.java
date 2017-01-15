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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.sqlpp.visitor.SqlppFormatPrintVisitor;

public class SqlppFormatPrintUtil {

    private SqlppFormatPrintUtil() {

    }

    /**
     * Prints the formatted output of an ILangExpression.
     *
     * @param expr
     *            the language expression.
     * @param output
     *            a writer for printing strings.
     * @throws CompilationException
     */
    public static void print(ILangExpression expr, PrintWriter output) throws CompilationException {
        SqlppFormatPrintVisitor visitor = new SqlppFormatPrintVisitor(output);
        expr.accept(visitor, 0);
    }

    /**
     * Prints the formatted output of a list of top-level language statements.
     *
     * @param statements
     *            a list of statements of a query
     * @param output
     *            a writer for printing strings.
     * @throws CompilationException
     */
    public static void print(List<Statement> statements, PrintWriter output) throws CompilationException {
        SqlppFormatPrintVisitor visitor = new SqlppFormatPrintVisitor(output);
        for (Statement statement : statements) {
            statement.accept(visitor, 0);
        }
    }

    /**
     * @param expr
     *            a language expression.
     * @return a formatted string of a language expression.
     * @throws CompilationException
     */
    public static String toString(ILangExpression expr) throws CompilationException {
        List<ILangExpression> exprs = new ArrayList<>();
        exprs.add(expr);
        return toString(exprs);
    }

    /**
     * @param exprs
     *            a list of language expression.
     * @return a formatted string of the input language expressions.
     * @throws CompilationException
     */
    public static String toString(List<ILangExpression> exprs) throws CompilationException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        PrintWriter output = new PrintWriter(bos);
        SqlppFormatPrintVisitor visitor = new SqlppFormatPrintVisitor(output);
        for (ILangExpression expr : exprs) {
            expr.accept(visitor, 0);
        }
        output.close();
        return bos.toString();
    }

}
