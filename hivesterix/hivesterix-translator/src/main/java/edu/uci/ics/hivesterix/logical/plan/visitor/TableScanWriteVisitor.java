/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hivesterix.logical.plan.visitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import edu.uci.ics.hivesterix.logical.plan.visitor.base.DefaultVisitor;
import edu.uci.ics.hivesterix.logical.plan.visitor.base.Translator;
import edu.uci.ics.hivesterix.runtime.jobgen.HiveDataSink;
import edu.uci.ics.hivesterix.runtime.jobgen.HiveDataSource;
import edu.uci.ics.hivesterix.runtime.jobgen.HiveMetaDataProvider;
import edu.uci.ics.hivesterix.runtime.jobgen.Schema;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSink;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSource;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;

public class TableScanWriteVisitor extends DefaultVisitor {

    /**
     * map from alias to partition desc
     */
    private HashMap<String, PartitionDesc> aliasToPathMap;

    /**
     * map from partition desc to data source
     */
    private HashMap<PartitionDesc, IDataSource<PartitionDesc>> dataSourceMap = new HashMap<PartitionDesc, IDataSource<PartitionDesc>>();

    /**
     * constructor
     * 
     * @param aliasToPathMap
     */
    public TableScanWriteVisitor(HashMap<String, PartitionDesc> aliasToPathMap) {
        this.aliasToPathMap = aliasToPathMap;
    }

    @Override
    public Mutable<ILogicalOperator> visit(TableScanOperator operator,
            Mutable<ILogicalOperator> AlgebricksParentOperator, Translator t) throws AlgebricksException {    	
        TableScanDesc desc = (TableScanDesc) operator.getConf();
        if (desc == null || desc.getAlias()==null) {
            List<LogicalVariable> schema = new ArrayList<LogicalVariable>();
            VariableUtilities.getLiveVariables(AlgebricksParentOperator.getValue(), schema);
            t.rewriteOperatorOutputSchema(schema, operator);
            return null;
        }

        List<ColumnInfo> columns = operator.getSchema().getSignature();
        for (int i = columns.size() - 1; i >= 0; i--)
            if (columns.get(i).getIsVirtualCol() == true)
                columns.remove(i);

        // start with empty tuple operator
        List<TypeInfo> types = new ArrayList<TypeInfo>();
        ArrayList<LogicalVariable> variables = new ArrayList<LogicalVariable>();
        List<String> names = new ArrayList<String>();
        for (ColumnInfo column : columns) {
            types.add(column.getType());

            LogicalVariable var = t.getVariableFromFieldName(column.getTabAlias() + "." + column.getInternalName());
            LogicalVariable varNew;

            if (var != null) {
                varNew = t.getVariable(column.getTabAlias() + "." + column.getInternalName() + operator.toString(),
                        column.getType());
                t.replaceVariable(var, varNew);
                var = varNew;
            } else
                var = t.getNewVariable(column.getTabAlias() + "." + column.getInternalName(), column.getType());

            variables.add(var);
            names.add(column.getInternalName());
        }
        Schema currentSchema = new Schema(names, types);

        String alias = desc.getAlias();
        PartitionDesc partDesc = aliasToPathMap.get(alias);
        IDataSource<PartitionDesc> dataSource = new HiveDataSource<PartitionDesc>(partDesc, currentSchema.getSchema());
        ILogicalOperator currentOperator = new DataSourceScanOperator(variables, dataSource);

        // set empty tuple source operator
        ILogicalOperator ets = new EmptyTupleSourceOperator();
        currentOperator.getInputs().add(new MutableObject<ILogicalOperator>(ets));

        // setup data source
        dataSourceMap.put(partDesc, dataSource);
        t.rewriteOperatorOutputSchema(variables, operator);
        return new MutableObject<ILogicalOperator>(currentOperator);
    }

    @Override
    public Mutable<ILogicalOperator> visit(FileSinkOperator hiveOperator,
            Mutable<ILogicalOperator> AlgebricksParentOperator, Translator t) {
        if (hiveOperator.getChildOperators() != null && hiveOperator.getChildOperators().size() > 0)
            return null;

        Schema currentSchema = t.generateInputSchema(hiveOperator.getParentOperators().get(0));

        IDataSink sink = new HiveDataSink(hiveOperator, currentSchema.getSchema());
        List<Mutable<ILogicalExpression>> exprList = new ArrayList<Mutable<ILogicalExpression>>();
        for (String column : currentSchema.getNames()) {
            exprList.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(t.getVariable(column))));
        }

        ILogicalOperator currentOperator = new WriteOperator(exprList, sink);
        if (AlgebricksParentOperator != null) {
            currentOperator.getInputs().add(AlgebricksParentOperator);
        }

        IMetadataProvider<PartitionDesc, Object> metaData = new HiveMetaDataProvider<PartitionDesc, Object>(
                hiveOperator, currentSchema, dataSourceMap);
        t.setMetadataProvider(metaData);
        return new MutableObject<ILogicalOperator>(currentOperator);
    }
}
