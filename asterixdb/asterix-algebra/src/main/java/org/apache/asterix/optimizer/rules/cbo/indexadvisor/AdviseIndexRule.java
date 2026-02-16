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
package org.apache.asterix.optimizer.rules.cbo.indexadvisor;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.declared.IIndexProvider;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.optimizer.rules.am.AccessMethodJobGenParams;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.IndexAdvisor;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestMapOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

// This rule executes after the CBO rule collections have run.
// By this point, the CBO should have generated a plan that includes fake indexes.
// This rule runs just once, is responsible for collecting and parsing the plan,
// inspecting each operator, identifying index scan operators,
// parses each operator to identify index scans, and adds the index details into the index advisor result.

//distribute result [$$16]
//  project ([$$16])
//      assign [$$16] <- [{"A": $$A}]
//          select (eq($$18, 1))
//              assign [$$18] <- [$$A.getField("b")]
//                  unnest-map [$$17, $$A] <- index-search("A", 0, "Default", "test", "A", false, false, 1, $$22, 1, $$22, true, true, true)
//                      order (ASC, $$22) [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
//                          select (eq($$21, 1))
//                              unnest-map [$$21, $$22] <- index-search("fake_index_4b4f5c05-f666-4ba2-912e-1a580de50542", 0, "Default", "test", "A", false, false, 1, $$19, 1, $$20, true, true, true)
//                                  assign [$$19, $$20] <- [1, 1]
//                                      empty-tuple-source

// This rule looks at unnest-map [$$21, $$22] <- index-search("fake_index_4b4f5c05-f666-4ba2-912e-1a580de50542", 0, "Default", "test", "A", false, false, 1, $$19, 1, $$20, true, true, true)
// and extracts the index name field names from the fake index provider.

public class AdviseIndexRule implements IAlgebraicRewriteRule {
    boolean applied = false;

    public AdviseIndexRule() {

    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (applied) {
            return false;
        }
        applied = true;

        if (!context.getIndexAdvisor().getAdvise()) {
            return false;
        }

        FakeIndexProvider fakeIndexProvider = (FakeIndexProvider) context.getIndexAdvisor().getFakeIndexProvider();
        IIndexProvider actualIndexProvider = (IIndexProvider) context.getMetadataProvider();
        visit(opRef, fakeIndexProvider, actualIndexProvider, context.getIndexAdvisor());

        return true;

    }

    void visit(Mutable<ILogicalOperator> opRef, IIndexProvider fakeIndexProvider, IIndexProvider actualIndexProvider,
            IndexAdvisor indexAdvisor) throws AlgebricksException {

        ILogicalOperator op = opRef.getValue();

        for (Mutable<ILogicalOperator> input : op.getInputs()) {
            visit(input, fakeIndexProvider, actualIndexProvider, indexAdvisor);
        }

        if (op.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP
                || op.getOperatorTag() == LogicalOperatorTag.LEFT_OUTER_UNNEST_MAP) {
            AbstractUnnestMapOperator unnestMapOp = (AbstractUnnestMapOperator) op;
            ILogicalExpression expr = unnestMapOp.getExpressionRef().getValue();
            if (!(expr instanceof UnnestingFunctionCallExpression unnestExpr)) {
                return;
            }

            AccessMethodJobGenParams jobGenParams = new AccessMethodJobGenParams();
            jobGenParams.readFromFuncArgs(unnestExpr.getArguments());
            if (jobGenParams.isPrimaryIndex()) {
                return;
            }
            if (jobGenParams.getIndexType() != DatasetConfig.IndexType.BTREE) {
                return;
            }

            String indexName = jobGenParams.getIndexName();
            String databaseName = jobGenParams.getDatabaseName();
            DataverseName dataverse = jobGenParams.getDataverseName();
            String datasetName = jobGenParams.getDatasetName();

            if (fakeIndexProvider == null) {
                // Case when CBO can't parse the plan correctly and the fake index provider is not set.
                return;
            }
            Index fakeIndex = fakeIndexProvider.getIndex(databaseName, dataverse, datasetName, indexName);
            if (fakeIndex == null) {
                // skips secondary primary index like
                // create primary index sec_primary_idx on A;
                return;
            }

            if (fakeIndex.getIndexDetails() instanceof Index.ValueIndexDetails) {
                Index actualIndex = lookupValueIndex(databaseName, dataverse, datasetName,
                        ((Index.ValueIndexDetails) fakeIndex.getIndexDetails()).getKeyFieldNames(),
                        actualIndexProvider);

                if (actualIndex != null
                        && actualIndex.getIndexDetails() instanceof Index.ValueIndexDetails valueIndexDetails) {
                    indexAdvisor.addPresentAdviseString(getCreateIndexClause(actualIndex.getIndexName(),
                            valueIndexDetails.getKeyFieldNames(), databaseName, dataverse, datasetName));
                } else {
                    indexAdvisor.addRecommendedAdviseString(getCreateIndexClause(
                            getIndexName(((Index.ValueIndexDetails) fakeIndex.getIndexDetails()).getKeyFieldNames()),
                            ((Index.ValueIndexDetails) fakeIndex.getIndexDetails()).getKeyFieldNames(), databaseName,
                            dataverse, datasetName));
                }

            } else if (fakeIndex.getIndexDetails() instanceof Index.ArrayIndexDetails fakeArrayIndexDetails) {
                Index actualIndex = lookupArrayIndex(databaseName, dataverse, datasetName,
                        ((Index.ArrayIndexDetails) fakeIndex.getIndexDetails()).getElementList(), actualIndexProvider);
                if (actualIndex != null
                        && actualIndex.getIndexDetails() instanceof Index.ArrayIndexDetails arrayIndexDetails) {
                    indexAdvisor.addPresentAdviseString(getCreateArrayIndexClause(actualIndex.getIndexName(),
                            arrayIndexDetails, databaseName, dataverse, datasetName));
                } else {

                    indexAdvisor.addRecommendedAdviseString(
                            getCreateArrayIndexClause(getArrayIndexName(fakeArrayIndexDetails.getElementList()),
                                    fakeArrayIndexDetails, databaseName, dataverse, datasetName));
                }
            }

        }

    }

    private static Index lookupArrayIndex(String databaseName, DataverseName dataverseName, String datasetName,
            List<Index.ArrayIndexElement> elementList, IIndexProvider indexProvider) throws AlgebricksException {
        return indexProvider.getDatasetIndexes(databaseName, dataverseName, datasetName).stream()
                .filter(index -> index.getIndexDetails() instanceof Index.ArrayIndexDetails)
                .filter(index -> (((Index.ArrayIndexDetails) index.getIndexDetails()).getElementList()
                        .equals(elementList)))
                .filter(index -> !index.isEnforced()).findFirst().orElse(null);
    }

    private static Index lookupValueIndex(String databaseName, DataverseName dataverseName, String datasetName,
            List<List<String>> fieldsNames, IIndexProvider indexProvider) throws AlgebricksException {
        return indexProvider.getDatasetIndexes(databaseName, dataverseName, datasetName).stream()
                .filter(index -> index.getIndexDetails() instanceof Index.ValueIndexDetails)
                .filter(index -> (((Index.ValueIndexDetails) index.getIndexDetails()).getKeyFieldNames()
                        .equals(fieldsNames)))
                .filter(index -> !index.isEnforced()).findFirst().orElse(null);
    }

    public static String getIndexName(List<List<String>> fields) {
        return "idx_adv_" + fields.stream().map(field -> String.join("_", field)).collect(Collectors.joining("_"))
                .replaceAll(" ", "");
    }

    public static String getArrayIndexName(List<Index.ArrayIndexElement> fields) {
        return "idx_adv_array_" + fields.stream().map(
                field -> field.getUnnestList().stream().map(f -> String.join("_", f)).collect(Collectors.joining("_")))
                .collect(Collectors.joining("_")).replaceAll(" ", "");
    }

    public static String getCreateArrayIndexClause(String indexName, Index.ArrayIndexDetails arrayIndexDetails,
            String databaseName, DataverseName dataverseName, String datasetName) {
        return "CREATE INDEX " + indexName + " ON `" + databaseName + "`.`" + dataverseName + "`.`" + datasetName + "`"
                + getArrayKeyFieldNamesClause(arrayIndexDetails) + " EXCLUDE UNKNOWN KEY;";
    }

    public static String getCreateIndexClause(String indexName, List<List<String>> keyFieldNames, String databaseName,
            DataverseName dataverseName, String datasetName) {
        return "CREATE INDEX " + indexName + " ON `" + databaseName + "`.`" + dataverseName + "`.`" + datasetName + "`"
                + getKeyFieldNamesClause(keyFieldNames) + ";";
    }

    public static String getArrayKeyFieldNamesClause(Index.ArrayIndexDetails arrayIndexDetails) {
        StringBuilder builder = new StringBuilder();
        builder.append("(");

        for (Index.ArrayIndexElement arrayIndexElement : arrayIndexDetails.getElementList()) {
            List<List<String>> unnestList = arrayIndexElement.getUnnestList();
            List<List<String>> projectList = arrayIndexElement.getProjectList();
            List<IAType> typeList = arrayIndexElement.getTypeList();
            int size = unnestList.size();
            for (int i = 0; i < size; i++) {
                builder.append("UNNEST ");
                builder.append(unnestList.get(i).stream().map(s -> "`" + s + "`").collect(Collectors.joining(".")));
                builder.append(" ");
            }

            if (projectList.isEmpty() || (projectList.size() == 1 && projectList.getFirst() == null)) {
                builder.append(": ");
                builder.append(typeList.getFirst());
            } else {

                for (int i = 0; i < projectList.size(); i++) {
                    if (i == 0) {
                        builder.append("SELECT ");
                    }
                    builder.append(
                            projectList.get(i).stream().map(s -> "`" + s + "`").collect(Collectors.joining(".")));
                    builder.append(": ");
                    builder.append(typeList.get(i));
                    if (i < projectList.size() - 1) {
                        builder.append(", ");
                    }
                }
            }
        }
        builder.append(")");
        return builder.toString();
    }

    public static String getKeyFieldNamesClause(List<List<String>> keyFieldNames) {
        return keyFieldNames.stream()
                .map(fields -> fields.stream().map(s -> "`" + s + "`").collect(Collectors.joining(".")))
                .collect(Collectors.joining(",", "(", ")"));

    }

}
