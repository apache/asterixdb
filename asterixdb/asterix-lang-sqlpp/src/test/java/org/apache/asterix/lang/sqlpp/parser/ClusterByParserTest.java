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

import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.NamespaceResolver;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.sqlpp.clause.ClusterbyClause;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.junit.Assert;
import org.junit.Test;

/**
 * Verifies that CLUSTER BY parses into a populated AST,
 * that GROUP BY is unaffected, and that CLUSTER remains a soft (non-reserved) keyword.
 */
public class ClusterByParserTest {

    private final IParserFactory factory = new SqlppParserFactory(new NamespaceResolver(false));

    private List<Statement> parse(String query) throws CompilationException {
        IParser parser = factory.createParser(query);
        return parser.parse();
    }

    private SelectBlock firstSelectBlock(List<Statement> stmts) {
        for (Statement s : stmts) {
            if (s instanceof Query) {
                Expression body = ((Query) s).getBody();
                Assert.assertTrue("query body should be a SelectExpression", body instanceof SelectExpression);
                return ((SelectExpression) body).getSelectSetOperation().getLeftInput().getSelectBlock();
            }
        }
        throw new AssertionError("no Query statement found");
    }

    @Test
    public void clusterByWithClusterAsAndWith() throws Exception {
        String q = "FROM Reviews r " + "CLUSTER BY r.reviewEmbedding AS sc " + "CLUSTER AS rvc "
                + "WITH { \"NumClusters\": 5, \"distanceFunction\": \"euclidean\" } "
                + "SELECT sc.cluster_id AS cid, array_count(rvc) AS cnt;";
        SelectBlock sb = firstSelectBlock(parse(q));
        Assert.assertTrue("clusterByClause should be populated", sb.hasClusterbyClause());
        Assert.assertFalse("groupbyClause should be absent", sb.hasGroupbyClause());
        ClusterbyClause cc = sb.getClusterbyClause();
        Assert.assertNotNull("clustering expression", cc.getClusteringExpression());
        Assert.assertNotNull("descriptor var (AS sc)", cc.getClusterDescriptorVar());
        Assert.assertTrue("members var (CLUSTER AS rvc)", cc.hasClusterMembersVar());
        Assert.assertTrue("WITH options", cc.hasWithOptions());
    }

    @Test
    public void clusterByWithoutClusterAsOrWith() throws Exception {
        String q = "FROM Reviews r CLUSTER BY r.reviewEmbedding AS sc SELECT sc.cluster_id AS cid;";
        SelectBlock sb = firstSelectBlock(parse(q));
        Assert.assertTrue(sb.hasClusterbyClause());
        ClusterbyClause cc = sb.getClusterbyClause();
        Assert.assertFalse("no CLUSTER AS", cc.hasClusterMembersVar());
        Assert.assertFalse("no WITH", cc.hasWithOptions());
    }

    @Test
    public void groupByStillParses() throws Exception {
        String q = "FROM Reviews r GROUP BY r.movie_id AS m GROUP AS g SELECT m;";
        SelectBlock sb = firstSelectBlock(parse(q));
        Assert.assertTrue("groupbyClause should be populated", sb.hasGroupbyClause());
        Assert.assertFalse("clusterByClause should be absent", sb.hasClusterbyClause());
    }

    @Test
    public void clusterIsSoftKeywordUsableAsIdentifier() throws Exception {
        // 'Cluster' as a dataset name and 'cluster' as field/alias must still parse (non-reserved).
        parse("SELECT c.x FROM Cluster AS c;");
        parse("SELECT cluster FROM ds;");
        parse("SELECT VALUE cluster FROM ds AS cluster;");
    }

    @Test(expected = CompilationException.class)
    public void groupByAndClusterByAreMutuallyExclusive() throws Exception {
        // The grammar permits only one of GROUP BY / CLUSTER BY per select block.
        parse("FROM Reviews r GROUP BY r.movie_id CLUSTER BY r.reviewEmbedding AS sc SELECT 1;");
    }
}