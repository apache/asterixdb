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
package org.apache.asterix.app.translator;

import static org.apache.asterix.lang.common.base.Statement.Category.DDL;
import static org.apache.asterix.lang.common.base.Statement.Category.QUERY;
import static org.apache.asterix.lang.common.base.Statement.Category.UPDATE;

import org.apache.asterix.common.metadata.NamespaceResolver;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.sqlpp.parser.SqlppParserFactory;
import org.junit.Assert;
import org.junit.Test;

public class StatementCategoriesTest {

    private static int categoriesOf(String request) throws Exception {
        IParser parser = new SqlppParserFactory(new NamespaceResolver(false)).createParser(request);
        return QueryTranslator.statementCategories(parser.parse());
    }

    @Test
    public void aSingleStatementHasItsOwnCategory() throws Exception {
        Assert.assertEquals(QUERY, categoriesOf("select 1;"));
        Assert.assertEquals(UPDATE, categoriesOf("upsert into ds ([{\"id\": 1}]);"));
        Assert.assertEquals(DDL, categoriesOf("create dataset ds primary key (id: string);"));
    }

    @Test
    public void aRequestHasTheCategoriesOfAllItsStatements() throws Exception {
        Assert.assertEquals(QUERY | DDL, categoriesOf("create dataset ds primary key (id: string); select 1;"));
        Assert.assertEquals(QUERY, categoriesOf("select 1; select 2;"));
    }

    /** Otherwise every request that sets its dataverse or a parameter would also be a query. */
    @Test
    public void declarationsDoNotCountAlongsideOtherStatements() throws Exception {
        Assert.assertEquals(DDL, categoriesOf("use dv; set `compiler.parallelism` \"1\"; drop dataset ds;"));
        Assert.assertEquals(UPDATE, categoriesOf("declare function f() { 1 }; upsert into ds ([{\"id\": 1}]);"));
    }

    @Test
    public void aRequestOfOnlyDeclarationsCountsThem() throws Exception {
        Assert.assertEquals(QUERY, categoriesOf("use dv;"));
    }
}
