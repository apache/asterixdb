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

import static java.util.UUID.randomUUID;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.metadata.DatasetFullyQualifiedName;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.declared.IIndexProvider;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;

public class FakeIndexProvider implements IIndexProvider {

    private final Map<DatasetFullyQualifiedName, Map<String, Index>> filterIndexesMap;
    private final IIndexEnumerator singleDataSourceIndexEnumerator = IIndexEnumerator.alphabeticEnumerator();
    private final IIndexEnumerator joinIndexEnumerator = IIndexEnumerator.alphabeticEnumerator();

    public FakeIndexProvider(CBOPlanStateTree planStateTree) {
        filterIndexesMap = new HashMap<>();
        if (planStateTree == null || planStateTree.getCboPlanNode() == null) {
            return;
        }

        addSingleDataSourceIndexes(planStateTree.getCboPlanNode().getLeafs());
        addArrayIndexes(planStateTree.getCboPlanNode().getLeafs());
        addJoinIndexes(planStateTree.getCboPlanNode().getJoins(), planStateTree.getDataSourceScanVariableMap());
    }

    private void addArrayIndexes(List<AdvisorScanPlanNode> scanPlanNodes) {
        for (AdvisorScanPlanNode scanPlanNode : scanPlanNodes) {
            DataSourceScanOperator scanOperator = scanPlanNode.getScanOperator();

            if (!(scanOperator.getDataSource() instanceof DatasetDataSource dataSource)) {
                continue;
            }
            DatasetFullyQualifiedName qualifiedName = dataSource.getDataset().getDatasetFullyQualifiedName();

            ScanFilter scanFilter = scanPlanNode.getFilter();
            List<UnnestFilterCondition> unnestFilterConditions = scanFilter.unnestFilterConditions();

            Map<List<List<String>>, Index.ArrayIndexElement> unnestToProjectListMap = new HashMap<>();
            Map<String, Index> datasetIndexes = filterIndexesMap.get(qualifiedName);
            if (datasetIndexes == null) {
                continue;
            }

            for (UnnestFilterCondition unnestFilterCondition : unnestFilterConditions) {
                List<List<String>> unnestList = unnestFilterCondition.getUnnestList();
                List<String> projectList = unnestFilterCondition.getProjectList();

                ConstantExpression constantExpression = unnestFilterCondition.getRhs();
                IAlgebricksConstantValue constantValue = constantExpression.getValue();
                if (!(constantValue instanceof AsterixConstantValue asterixConstantValue)) {
                    continue;
                }
                IAType iaType = asterixConstantValue.getObject().getType();

                Index.ArrayIndexElement arrayIndexElement = unnestToProjectListMap.get(unnestList);
                if (arrayIndexElement != null) {
                    if (!arrayIndexElement.getProjectList().contains(projectList)) {
                        arrayIndexElement.getProjectList().add(projectList);
                        arrayIndexElement.getTypeList().add(iaType);
                    }
                } else {
                    List<List<String>> projectListWrapper = new ArrayList<>();
                    projectListWrapper.add(projectList.isEmpty() ? null : projectList);
                    List<IAType> typeList = new ArrayList<>();
                    typeList.add(iaType);

                    arrayIndexElement = new Index.ArrayIndexElement(unnestList, projectListWrapper, typeList, 0);
                    unnestToProjectListMap.put(unnestList, arrayIndexElement);
                }
            }

            for (Map.Entry<List<List<String>>, Index.ArrayIndexElement> entry : unnestToProjectListMap.entrySet()) {
                String indexName = "fake_array_index_" + randomUUID();
                FakeIndex fakeIndex = new FakeIndex(qualifiedName.getDatabaseName(), qualifiedName.getDataverseName(),
                        qualifiedName.getDatasetName(), indexName, entry.getValue());
                datasetIndexes.put(indexName, fakeIndex);
            }
        }
    }

    private void addSingleDataSourceIndexes(List<AdvisorScanPlanNode> scanPlanNodes) {
        Map<DatasetFullyQualifiedName, Pair<DataSourceScanOperator, Set<List<String>>>> singleDataSourceFieldNamesMap =
                new HashMap<>();
        for (AdvisorScanPlanNode scanPlanNode : scanPlanNodes) {
            DataSourceScanOperator scanOperator = scanPlanNode.getScanOperator();

            if (!(scanOperator.getDataSource() instanceof DatasetDataSource dataSource)) {
                continue;
            }
            DatasetFullyQualifiedName fullyQualifiedName = dataSource.getDataset().getDatasetFullyQualifiedName();

            ScanFilter filter = scanPlanNode.getFilter();
            List<ScanFilterCondition> filterConditions = filter.filterConditions();
            HashSet<List<String>> datasetFieldNames = new HashSet<>();
            for (ScanFilterCondition filterCondition : filterConditions) {
                if (scanOperator.getScanVariables().contains(filterCondition.getScanVar())) {
                    datasetFieldNames.add(filterCondition.getLhsFieldAccessPath());
                }

            }
            singleDataSourceFieldNamesMap.put(fullyQualifiedName, new Pair<>(scanOperator, datasetFieldNames));

        }
        for (Map.Entry<DatasetFullyQualifiedName, Pair<DataSourceScanOperator, Set<List<String>>>> entry : singleDataSourceFieldNamesMap
                .entrySet()) {
            DatasetFullyQualifiedName qualifiedName = entry.getKey();
            Set<List<String>> fieldNames = entry.getValue().getSecond();
            DataSourceScanOperator scanOperator = entry.getValue().getFirst();

            if (((DatasetDataSource) scanOperator.getDataSource()).getDataset().getDatasetDetails()
                    .getDatasetType() != DatasetConfig.DatasetType.INTERNAL) {
                continue;
            }

            List<List<String>> primaryKeys =
                    ((InternalDatasetDetails) ((DatasetDataSource) scanOperator.getDataSource()).getDataset()
                            .getDatasetDetails()).getPrimaryKey();

            Map<String, Index> datasetIndexes = new HashMap<>();
            filterIndexesMap.put(qualifiedName, datasetIndexes);

            String primaryKeyName = ((DataSourceId) scanOperator.getDataSource().getId()).getDatasourceName();

            FakeIndex primIndex = new FakeIndex(qualifiedName.getDatabaseName(), qualifiedName.getDataverseName(),
                    qualifiedName.getDatasetName(), primaryKeyName, primaryKeys, true);
            datasetIndexes.put(primaryKeyName, primIndex);

            if (fieldNames.isEmpty()) {
                continue;
            }

            singleDataSourceIndexEnumerator.init(fieldNames);
            Iterator<List<List<String>>> itr = singleDataSourceIndexEnumerator.getIterator();

            while (itr.hasNext()) {
                String indexName = "fake_index_" + randomUUID();
                FakeIndex fakeIndex = new FakeIndex(qualifiedName.getDatabaseName(), qualifiedName.getDataverseName(),
                        qualifiedName.getDatasetName(), indexName, itr.next(), false);
                datasetIndexes.put(indexName, fakeIndex);
            }
        }
    }

    private void addJoinIndexes(List<AdvisorJoinPlanNode> joinPlanNodes,
            Map<LogicalVariable, DataSourceScanOperator> dataSourceScanVariableMap) {
        Map<DatasetFullyQualifiedName, Set<List<String>>> joinDataSourceFieldNamesMap = new HashMap<>();
        for (AdvisorJoinPlanNode joinPlanNode : joinPlanNodes) {
            for (JoinFilterCondition filterCondition : joinPlanNode.getJoinCondition().joinFilterConditions()) {
                addOperandToMap(filterCondition.getLhsVar(), filterCondition.getLhsFields(),
                        joinDataSourceFieldNamesMap, dataSourceScanVariableMap);
                addOperandToMap(filterCondition.getRhsVar(), filterCondition.getRhsFields(),
                        joinDataSourceFieldNamesMap, dataSourceScanVariableMap);
            }

        }
        for (Map.Entry<DatasetFullyQualifiedName, Set<List<String>>> entry : joinDataSourceFieldNamesMap.entrySet()) {
            DatasetFullyQualifiedName qualifiedName = entry.getKey();
            Set<List<String>> fieldNames = entry.getValue();
            if (fieldNames.isEmpty()) {
                continue;
            }

            joinIndexEnumerator.init(fieldNames);
            Iterator<List<List<String>>> itr = joinIndexEnumerator.getIterator();
            Map<String, Index> indexes = filterIndexesMap.get(qualifiedName);

            while (itr.hasNext()) {
                String indexName = "fake_join_index_" + randomUUID();
                FakeIndex fakeIndex = new FakeIndex(qualifiedName.getDatabaseName(), qualifiedName.getDataverseName(),
                        qualifiedName.getDatasetName(), indexName, itr.next(), false);
                indexes.put(indexName, fakeIndex);
            }

        }
    }

    private void addOperandToMap(LogicalVariable var, List<String> fields,
            Map<DatasetFullyQualifiedName, Set<List<String>>> joinDataSourceFieldNamesMap,
            Map<LogicalVariable, DataSourceScanOperator> dataSourceScanVariableMap) {
        DataSourceScanOperator op = dataSourceScanVariableMap.get(var);
        if (op == null) {
            return;
        }
        if (!(op.getDataSource() instanceof DatasetDataSource dataSource)) {
            return;
        }
        DatasetFullyQualifiedName qualifiedName = dataSource.getDataset().getDatasetFullyQualifiedName();
        joinDataSourceFieldNamesMap.computeIfAbsent(qualifiedName, k -> new HashSet<>()).add(fields);
    }

    @Override
    public Index getIndex(String database, DataverseName dataverseName, String datasetName, String indexName)
            throws AlgebricksException {
        return filterIndexesMap.get(new DatasetFullyQualifiedName(database, dataverseName, datasetName)).get(indexName);
    }

    @Override
    public List<Index> getDatasetIndexes(String database, DataverseName dataverseName, String datasetName)
            throws AlgebricksException {
        return filterIndexesMap.getOrDefault(new DatasetFullyQualifiedName(database, dataverseName, datasetName),
                Collections.emptyMap()).values().stream().toList();
    }

}
