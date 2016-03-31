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
package org.apache.asterix.test.querygen;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.List;

import org.apache.asterix.lang.aql.parser.AQLParserFactory;
import org.apache.asterix.lang.aql.util.AQLFormatPrintUtil;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.sqlpp.parser.SqlppParserFactory;
import org.apache.commons.io.FileUtils;

public class AQLToSQLPPConverter {

    private final static IParserFactory aqlParserFactory = new AQLParserFactory();
    private final static IParserFactory sqlppParserFactory = new SqlppParserFactory();

    public static void convert(String dirName) throws Exception {
        File dir = new File(dirName);
        File target = new File(dirName + "_sqlpp");
        FileUtils.deleteQuietly(target);
        FileUtils.forceMkdir(target);
        convert(dir, target);
    }

    private static void convert(File src, File dest) throws Exception {
        if (src.isFile()) {
            BufferedReader parserReader = new BufferedReader(new InputStreamReader(new FileInputStream(src)));
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(src)));
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dest)));
            try {
                String line = null;
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith("/*") || line.startsWith(" *") || line.startsWith("*") || line.startsWith("\t")
                            || line.startsWith(" \t")) {
                        writer.write(line + "\n");
                    } else {
                        break;
                    }
                }
                writer.write("\n");
                reader.close();

                IParser parser = aqlParserFactory.createParser(parserReader);
                List<Statement> statements = parser.parse();
                parserReader.close();

                String sqlString = AQLFormatPrintUtil.toSQLPPString(statements);
                writer.write(sqlString);
            } catch (Exception e) {
                System.out.println("AQL parser fails at: " + src.getAbsolutePath());
                //e.printStackTrace();
            } finally {
                parserReader.close();
                reader.close();
                writer.close();
            }

            BufferedReader sqlReader = new BufferedReader(new InputStreamReader(new FileInputStream(dest)));
            try {
                IParser sqlParser = sqlppParserFactory.createParser(sqlReader);
                sqlParser.parse();
            } catch (Exception e) {
                System.out.println("SQL++ parser cannot parse: ");
                System.out.println(dest.getAbsolutePath());
                e.printStackTrace();
            } finally {
                sqlReader.close();
            }
            return;
        }
        for (File child : src.listFiles()) {
            String lastName = child.getName();
            lastName = lastName.replaceAll("\\.aql", "\\.sqlpp");
            File targetChild = new File(dest, lastName);
            if (child.isDirectory()) {
                FileUtils.forceMkdir(targetChild);
            } else {
                targetChild.createNewFile();
            }
            convert(child, targetChild);
        }
    }

    public static void main(String[] args) throws Exception {
        convert("src/test/resources/runtimets/queries");
        convert("src/test/resources/optimizerts/queries");
    }

}
