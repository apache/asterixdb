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
package edu.uci.ics.hivesterix.logical.plan.visitor.base;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.UDTFDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import edu.uci.ics.hivesterix.runtime.jobgen.Schema;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

@SuppressWarnings("rawtypes")
public interface Translator {

    /**
     * generate input schema
     * 
     * @param operator
     * @return
     */
    public Schema generateInputSchema(Operator operator);

    /**
     * rewrite the names of output columns for feture expression evaluators to
     * use
     * 
     * @param operator
     */
    public void rewriteOperatorOutputSchema(List<LogicalVariable> vars, Operator operator);

    /**
     * rewrite the names of output columns for feture expression evaluators to
     * use
     * 
     * @param operator
     */
    public void rewriteOperatorOutputSchema(Operator operator);

    /**
     * rewrite an expression and substitute variables
     * 
     * @param expr
     *            hive expression
     */
    public void rewriteExpression(ExprNodeDesc expr);

    /**
     * rewrite an expression and substitute variables
     * 
     * @param expr
     *            hive expression
     */
    public void rewriteExpressionPartial(ExprNodeDesc expr);

    /**
     * get an assign operator as a child of parent
     * 
     * @param parent
     * @param cols
     * @param variables
     * @return
     */
    public ILogicalOperator getAssignOperator(Mutable<ILogicalOperator> parent, List<ExprNodeDesc> cols,
            ArrayList<LogicalVariable> variables);

    /**
     * get type for a logical variable
     * 
     * @param var
     * @return type info
     */
    public TypeInfo getType(LogicalVariable var);

    /**
     * translate an expression from hive to Algebricks
     * 
     * @param desc
     * @return
     */
    public Mutable<ILogicalExpression> translateScalarFucntion(ExprNodeDesc desc);

    /**
     * translate an aggregation from hive to Algebricks
     * 
     * @param aggregateDesc
     * @return
     */
    public Mutable<ILogicalExpression> translateAggregation(AggregationDesc aggregateDesc);

    /**
     * translate unnesting (UDTF) function expression
     * 
     * @param aggregator
     * @return
     */
    public Mutable<ILogicalExpression> translateUnnestFunction(UDTFDesc udtfDesc, Mutable<ILogicalExpression> argument);

    /**
     * get variable from a schema
     * 
     * @param schema
     * @return
     */
    public List<LogicalVariable> getVariablesFromSchema(Schema schema);

    /**
     * get variable from name
     * 
     * @param name
     * @return
     */
    public LogicalVariable getVariable(String name);

    /**
     * get variable from field name
     * 
     * @param name
     * @return
     */
    public LogicalVariable getVariableFromFieldName(String name);

    /**
     * get variable from name, type
     * 
     * @param fieldName
     * @param type
     * @return
     */
    public LogicalVariable getVariable(String fieldName, TypeInfo type);

    /**
     * get new variable from name, type
     * 
     * @param fieldName
     * @param type
     * @return
     */
    public LogicalVariable getNewVariable(String fieldName, TypeInfo type);

    /**
     * update a name-variable binding
     * 
     * @param fieldName
     * @param variable
     */
    public void updateVariable(String fieldName, LogicalVariable variable);

    /**
     * set the metadata provider
     * 
     * @param metadata
     */
    public void setMetadataProvider(IMetadataProvider<PartitionDesc, Object> metadata);

    /**
     * get the metadata provider
     * 
     * @param metadata
     */
    public IMetadataProvider<PartitionDesc, Object> getMetadataProvider();

    /**
     * replace the variable
     * 
     * @param oldVar
     * @param newVar
     */
    public void replaceVariable(LogicalVariable oldVar, LogicalVariable newVar);

}
