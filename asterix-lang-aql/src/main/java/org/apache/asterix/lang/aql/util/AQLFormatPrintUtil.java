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
package org.apache.asterix.lang.aql.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.aql.visitor.AQLFormatPrintVisitor;
import org.apache.asterix.lang.aql.visitor.AQLToSQLPPPrintVisitor;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.base.Statement;

public class AQLFormatPrintUtil {

    public static void print(ILangExpression expr, PrintWriter output) throws AsterixException {
        AQLFormatPrintVisitor visitor = new AQLFormatPrintVisitor(output);
        expr.accept(visitor, 0);
    }

    public static void print(List<Statement> exprs, PrintWriter output) throws AsterixException {
        AQLFormatPrintVisitor visitor = new AQLFormatPrintVisitor(output);
        for (Statement expr : exprs) {
            expr.accept(visitor, 0);
        }
    }

    public static String toString(List<Statement> exprs) throws AsterixException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        PrintWriter output = new PrintWriter(bos);
        AQLFormatPrintVisitor visitor = new AQLFormatPrintVisitor(output);
        for (Statement expr : exprs) {
            expr.accept(visitor, 0);
        }
        output.close();
        return new String(bos.toByteArray());
    }

    public static String toSQLPPString(List<Statement> exprs) throws AsterixException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        PrintWriter output = new PrintWriter(bos);
        AQLToSQLPPPrintVisitor visitor = new AQLToSQLPPPrintVisitor(output);
        for (Statement expr : exprs) {
            expr.accept(visitor, 0);
        }
        output.close();
        return new String(bos.toByteArray());
    }
}
