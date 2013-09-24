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
package edu.uci.ics.hivesterix.logical.plan;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.ExtractOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewJoinOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDTFOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeNullDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.UDTFDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import edu.uci.ics.hivesterix.logical.expression.ExpressionConstant;
import edu.uci.ics.hivesterix.logical.expression.HiveAlgebricksBuiltInFunctionMap;
import edu.uci.ics.hivesterix.logical.expression.HiveFunctionInfo;
import edu.uci.ics.hivesterix.logical.expression.HivesterixConstantValue;
import edu.uci.ics.hivesterix.logical.plan.visitor.ExtractVisitor;
import edu.uci.ics.hivesterix.logical.plan.visitor.FilterVisitor;
import edu.uci.ics.hivesterix.logical.plan.visitor.GroupByVisitor;
import edu.uci.ics.hivesterix.logical.plan.visitor.JoinVisitor;
import edu.uci.ics.hivesterix.logical.plan.visitor.LateralViewJoinVisitor;
import edu.uci.ics.hivesterix.logical.plan.visitor.LimitVisitor;
import edu.uci.ics.hivesterix.logical.plan.visitor.MapJoinVisitor;
import edu.uci.ics.hivesterix.logical.plan.visitor.ProjectVisitor;
import edu.uci.ics.hivesterix.logical.plan.visitor.SortVisitor;
import edu.uci.ics.hivesterix.logical.plan.visitor.TableScanWriteVisitor;
import edu.uci.ics.hivesterix.logical.plan.visitor.UnionVisitor;
import edu.uci.ics.hivesterix.logical.plan.visitor.base.Translator;
import edu.uci.ics.hivesterix.logical.plan.visitor.base.Visitor;
import edu.uci.ics.hivesterix.runtime.jobgen.Schema;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import edu.uci.ics.hyracks.algebricks.core.algebra.prettyprint.LogicalOperatorPrettyPrintVisitor;
import edu.uci.ics.hyracks.algebricks.core.algebra.prettyprint.PlanPrettyPrinter;

@SuppressWarnings("rawtypes")
public class HiveAlgebricksTranslator implements Translator {

    private int currentVariable = 0;

    private List<Mutable<ILogicalOperator>> logicalOp = new ArrayList<Mutable<ILogicalOperator>>();

    private boolean continueTraverse = true;

    private IMetadataProvider<PartitionDesc, Object> metaData;

    /**
     * map variable name to the logical variable
     */
    private HashMap<String, LogicalVariable> nameToLogicalVariableMap = new HashMap<String, LogicalVariable>();

    /**
     * map field name to LogicalVariable
     */
    private HashMap<String, LogicalVariable> fieldToLogicalVariableMap = new HashMap<String, LogicalVariable>();

    /**
     * map logical variable to name
     */
    private HashMap<LogicalVariable, String> logicalVariableToFieldMap = new HashMap<LogicalVariable, String>();

    /**
     * asterix root operators
     */
    private List<Mutable<ILogicalOperator>> rootOperators = new ArrayList<Mutable<ILogicalOperator>>();

    /**
     * a list of visitors
     */
    private List<Visitor> visitors = new ArrayList<Visitor>();

    /**
     * output writer to print things out
     */
    private static PrintWriter outputWriter = new PrintWriter(new OutputStreamWriter(System.out));

    /**
     * map a logical variable to type info
     */
    private HashMap<LogicalVariable, TypeInfo> variableToType = new HashMap<LogicalVariable, TypeInfo>();

    @Override
    public LogicalVariable getVariable(String fieldName, TypeInfo type) {
        LogicalVariable var = fieldToLogicalVariableMap.get(fieldName);
        if (var == null) {
            currentVariable++;
            var = new LogicalVariable(currentVariable);
            fieldToLogicalVariableMap.put(fieldName, var);
            nameToLogicalVariableMap.put(var.toString(), var);
            variableToType.put(var, type);
            logicalVariableToFieldMap.put(var, fieldName);
        }
        return var;
    }

    @Override
    public LogicalVariable getNewVariable(String fieldName, TypeInfo type) {
        currentVariable++;
        LogicalVariable var = new LogicalVariable(currentVariable);
        fieldToLogicalVariableMap.put(fieldName, var);
        nameToLogicalVariableMap.put(var.toString(), var);
        variableToType.put(var, type);
        logicalVariableToFieldMap.put(var, fieldName);
        return var;
    }

    @Override
    public void replaceVariable(LogicalVariable oldVar, LogicalVariable newVar) {
        String name = this.logicalVariableToFieldMap.get(oldVar);
        if (name != null) {
            fieldToLogicalVariableMap.put(name, newVar);
            nameToLogicalVariableMap.put(newVar.toString(), newVar);
            nameToLogicalVariableMap.put(oldVar.toString(), newVar);
            logicalVariableToFieldMap.put(newVar, name);
        }
    }

    @Override
    public IMetadataProvider<PartitionDesc, Object> getMetadataProvider() {
        return metaData;
    }

    /**
     * only get an variable, without rewriting it
     * 
     * @param fieldName
     * @return
     */
    private LogicalVariable getVariableOnly(String fieldName) {
        return fieldToLogicalVariableMap.get(fieldName);
    }

    public void updateVariable(String fieldName, LogicalVariable variable) {
        LogicalVariable var = fieldToLogicalVariableMap.get(fieldName);
        if (var == null) {
            fieldToLogicalVariableMap.put(fieldName, variable);
            nameToLogicalVariableMap.put(fieldName, variable);
        } else if (!var.equals(variable)) {
            fieldToLogicalVariableMap.put(fieldName, variable);
            nameToLogicalVariableMap.put(fieldName, variable);
        }
    }

    /**
     * get a list of logical variables from the schema
     * 
     * @param schema
     * @return
     */
    @Override
    public List<LogicalVariable> getVariablesFromSchema(Schema schema) {
        List<LogicalVariable> variables = new ArrayList<LogicalVariable>();
        List<String> names = schema.getNames();

        for (String name : names)
            variables.add(nameToLogicalVariableMap.get(name));
        return variables;
    }

    /**
     * get variable to typeinfo map
     * 
     * @return
     */
    public HashMap<LogicalVariable, TypeInfo> getVariableContext() {
        return this.variableToType;
    }

    /**
     * get the number of variables s
     * 
     * @return
     */
    public int getVariableCounter() {
        return currentVariable + 1;
    }

    /**
     * translate from hive operator tree to asterix operator tree
     * 
     * @param hive
     *            roots
     * @return Algebricks roots
     */
    public void translate(List<Operator> hiveRoot, ILogicalOperator parentOperator,
            HashMap<String, PartitionDesc> aliasToPathMap) throws AlgebricksException {
        /**
         * register visitors
         */
        visitors.add(new FilterVisitor());
        visitors.add(new GroupByVisitor());
        visitors.add(new JoinVisitor());
        visitors.add(new LateralViewJoinVisitor());
        visitors.add(new UnionVisitor());
        visitors.add(new LimitVisitor());
        visitors.add(new MapJoinVisitor());
        visitors.add(new ProjectVisitor());
        visitors.add(new SortVisitor());
        visitors.add(new ExtractVisitor());
        visitors.add(new TableScanWriteVisitor(aliasToPathMap));

        List<Mutable<ILogicalOperator>> refList = translate(hiveRoot, new MutableObject<ILogicalOperator>(
                parentOperator));
        insertReplicateOperator(refList);
        if (refList != null)
            rootOperators.addAll(refList);
    }

    /**
     * translate operator DAG
     * 
     * @param hiveRoot
     * @param AlgebricksParentOperator
     * @return
     */
    private List<Mutable<ILogicalOperator>> translate(List<Operator> hiveRoot,
            Mutable<ILogicalOperator> AlgebricksParentOperator) throws AlgebricksException {

        for (Operator hiveOperator : hiveRoot) {
            continueTraverse = true;
            Mutable<ILogicalOperator> currentOperatorRef = null;
            if (hiveOperator.getType() == OperatorType.FILTER) {
                FilterOperator fop = (FilterOperator) hiveOperator;
                for (Visitor visitor : visitors) {
                    currentOperatorRef = visitor.visit(fop, AlgebricksParentOperator, this);
                    if (currentOperatorRef != null)
                        break;
                }
            } else if (hiveOperator.getType() == OperatorType.REDUCESINK) {
                ReduceSinkOperator fop = (ReduceSinkOperator) hiveOperator;
                for (Visitor visitor : visitors) {
                    currentOperatorRef = visitor.visit(fop, AlgebricksParentOperator, this);
                    if (currentOperatorRef != null)
                        break;
                }
            } else if (hiveOperator.getType() == OperatorType.JOIN) {
                JoinOperator fop = (JoinOperator) hiveOperator;
                for (Visitor visitor : visitors) {
                    currentOperatorRef = visitor.visit(fop, AlgebricksParentOperator, this);
                    if (currentOperatorRef != null) {
                        continueTraverse = true;
                        break;
                    } else
                        continueTraverse = false;
                }
                if (currentOperatorRef == null)
                    return null;
            } else if (hiveOperator.getType() == OperatorType.LATERALVIEWJOIN) {
                LateralViewJoinOperator fop = (LateralViewJoinOperator) hiveOperator;
                for (Visitor visitor : visitors) {
                    currentOperatorRef = visitor.visit(fop, AlgebricksParentOperator, this);
                    if (currentOperatorRef != null)
                        break;
                }
                if (currentOperatorRef == null)
                    return null;
            } else if (hiveOperator.getType() == OperatorType.MAPJOIN) {
                MapJoinOperator fop = (MapJoinOperator) hiveOperator;
                for (Visitor visitor : visitors) {
                    currentOperatorRef = visitor.visit(fop, AlgebricksParentOperator, this);
                    if (currentOperatorRef != null) {
                        continueTraverse = true;
                        break;
                    } else
                        continueTraverse = false;
                }
                if (currentOperatorRef == null)
                    return null;
            } else if (hiveOperator.getType() == OperatorType.SELECT) {
                SelectOperator fop = (SelectOperator) hiveOperator;
                for (Visitor visitor : visitors) {
                    currentOperatorRef = visitor.visit(fop, AlgebricksParentOperator, this);
                    if (currentOperatorRef != null)
                        break;
                }
            } else if (hiveOperator.getType() == OperatorType.EXTRACT) {
                ExtractOperator fop = (ExtractOperator) hiveOperator;
                for (Visitor visitor : visitors) {
                    currentOperatorRef = visitor.visit(fop, AlgebricksParentOperator, this);
                    if (currentOperatorRef != null)
                        break;
                }
            } else if (hiveOperator.getType() == OperatorType.GROUPBY) {
                GroupByOperator fop = (GroupByOperator) hiveOperator;
                for (Visitor visitor : visitors) {
                    currentOperatorRef = visitor.visit(fop, AlgebricksParentOperator, this);
                    if (currentOperatorRef != null)
                        break;
                }
            } else if (hiveOperator.getType() == OperatorType.TABLESCAN) {
                TableScanOperator fop = (TableScanOperator) hiveOperator;
                for (Visitor visitor : visitors) {
                    currentOperatorRef = visitor.visit(fop, AlgebricksParentOperator, this);
                    if (currentOperatorRef != null)
                        break;
                }
            } else if (hiveOperator.getType() == OperatorType.FILESINK) {
                FileSinkOperator fop = (FileSinkOperator) hiveOperator;
                for (Visitor visitor : visitors) {
                    currentOperatorRef = visitor.visit(fop, AlgebricksParentOperator, this);
                    if (currentOperatorRef != null)
                        break;
                }
            } else if (hiveOperator.getType() == OperatorType.LIMIT) {
                LimitOperator lop = (LimitOperator) hiveOperator;
                for (Visitor visitor : visitors) {
                    currentOperatorRef = visitor.visit(lop, AlgebricksParentOperator, this);
                    if (currentOperatorRef != null)
                        break;
                }
            } else if (hiveOperator.getType() == OperatorType.UDTF) {
                UDTFOperator lop = (UDTFOperator) hiveOperator;
                for (Visitor visitor : visitors) {
                    currentOperatorRef = visitor.visit(lop, AlgebricksParentOperator, this);
                    if (currentOperatorRef != null)
                        break;
                }
            } else if (hiveOperator.getType() == OperatorType.UNION) {
                UnionOperator lop = (UnionOperator) hiveOperator;
                for (Visitor visitor : visitors) {
                    currentOperatorRef = visitor.visit(lop, AlgebricksParentOperator, this);
                    if (currentOperatorRef != null) {
                        continueTraverse = true;
                        break;
                    } else
                        continueTraverse = false;
                }
            } else
                ;
            if (hiveOperator.getChildOperators() != null && hiveOperator.getChildOperators().size() > 0
                    && continueTraverse) {
                @SuppressWarnings("unchecked")
                List<Operator> children = hiveOperator.getChildOperators();
                if (currentOperatorRef == null)
                    currentOperatorRef = AlgebricksParentOperator;
                translate(children, currentOperatorRef);
            }
            if (hiveOperator.getChildOperators() == null || hiveOperator.getChildOperators().size() == 0)
                logicalOp.add(currentOperatorRef);
        }
        return logicalOp;
    }

    /**
     * used in select, group by to get no-column-expression columns
     * 
     * @param cols
     * @return
     */
    public ILogicalOperator getAssignOperator(Mutable<ILogicalOperator> parent, List<ExprNodeDesc> cols,
            ArrayList<LogicalVariable> variables) {

        ArrayList<Mutable<ILogicalExpression>> expressions = new ArrayList<Mutable<ILogicalExpression>>();

        /**
         * variables to be appended in the assign operator
         */
        ArrayList<LogicalVariable> appendedVariables = new ArrayList<LogicalVariable>();

        // one variable can only be assigned once
        for (ExprNodeDesc hiveExpr : cols) {
            rewriteExpression(hiveExpr);

            if (hiveExpr instanceof ExprNodeColumnDesc) {
                ExprNodeColumnDesc desc2 = (ExprNodeColumnDesc) hiveExpr;
                String fieldName = desc2.getTabAlias() + "." + desc2.getColumn();

                // System.out.println("project expr: " + fieldName);

                if (fieldName.indexOf("$$") < 0) {
                    LogicalVariable var = getVariable(fieldName, hiveExpr.getTypeInfo());
                    desc2.setColumn(var.toString());
                    desc2.setTabAlias("");
                    variables.add(var);
                } else {
                    LogicalVariable var = nameToLogicalVariableMap.get(desc2.getColumn());
                    String name = this.logicalVariableToFieldMap.get(var);
                    var = this.getVariableOnly(name);
                    variables.add(var);
                }
            } else {
                Mutable<ILogicalExpression> asterixExpr = translateScalarFucntion(hiveExpr);
                expressions.add(asterixExpr);
                LogicalVariable var = getVariable(hiveExpr.getExprString() + asterixExpr.hashCode(),
                        hiveExpr.getTypeInfo());
                variables.add(var);
                appendedVariables.add(var);
            }
        }

        /**
         * create an assign operator to deal with appending
         */
        ILogicalOperator assignOp = null;
        if (appendedVariables.size() > 0) {
            assignOp = new AssignOperator(appendedVariables, expressions);
            assignOp.getInputs().add(parent);
        }
        return assignOp;
    }

    private ILogicalPlan plan;

    public ILogicalPlan genLogicalPlan() {
        plan = new ALogicalPlanImpl(rootOperators);
        return plan;
    }

    public void printOperators() throws AlgebricksException {
        LogicalOperatorPrettyPrintVisitor pvisitor = new LogicalOperatorPrettyPrintVisitor();
        StringBuilder buffer = new StringBuilder();
        PlanPrettyPrinter.printPlan(plan, buffer, pvisitor, 0);
        outputWriter.println(buffer);
        outputWriter.println("rewritten variables: ");
        outputWriter.flush();
        printVariables();

    }

    public static void setOutputPrinter(PrintWriter writer) {
        outputWriter = writer;
    }

    private void printVariables() {
        Set<Entry<String, LogicalVariable>> entries = fieldToLogicalVariableMap.entrySet();

        for (Entry<String, LogicalVariable> entry : entries) {
            outputWriter.println(entry.getKey() + " -> " + entry.getValue());
        }
        outputWriter.flush();
    }

    /**
     * generate the object inspector for the output of an operator
     * 
     * @param operator
     *            The Hive operator
     * @return an ObjectInspector object
     */
    public Schema generateInputSchema(Operator operator) {
        List<String> variableNames = new ArrayList<String>();
        List<TypeInfo> typeList = new ArrayList<TypeInfo>();
        List<ColumnInfo> columns = operator.getSchema().getSignature();

        for (ColumnInfo col : columns) {
            // typeList.add();
            TypeInfo type = col.getType();
            typeList.add(type);

            String fieldName = col.getInternalName();
            variableNames.add(fieldName);
        }

        return new Schema(variableNames, typeList);
    }

    /**
     * rewrite the names of output columns for feature expression evaluators to
     * use
     * 
     * @param operator
     */
    public void rewriteOperatorOutputSchema(Operator operator) {
        List<ColumnInfo> columns = operator.getSchema().getSignature();
        for (ColumnInfo column : columns) {
            String columnName = column.getTabAlias() + "." + column.getInternalName();
            if (columnName.indexOf("$$") < 0) {
                LogicalVariable var = getVariable(columnName, column.getType());
                column.setInternalName(var.toString());
            }
        }
    }

    @Override
    public void rewriteOperatorOutputSchema(List<LogicalVariable> variables, Operator operator) {
        // printOperatorSchema(operator);
        List<ColumnInfo> columns = operator.getSchema().getSignature();
        // if (variables.size() != columns.size()) {
        // throw new IllegalStateException("output cardinality error " +
        // operator.getName() + " variable size: "
        // + variables.size() + " expected " + columns.size());
        // }
        for (int i = 0; i < variables.size(); i++) {
            LogicalVariable var = variables.get(i);
            ColumnInfo column = columns.get(i);
            String fieldName = column.getTabAlias() + "." + column.getInternalName();
            if (fieldName.indexOf("$$") < 0) {
                updateVariable(fieldName, var);
                column.setInternalName(var.toString());
            }
        }

        // printOperatorSchema(operator);
    }

    /**
     * rewrite an expression and substitute variables
     * 
     * @param expr
     *            hive expression
     */
    public void rewriteExpression(ExprNodeDesc expr) {
        if (expr instanceof ExprNodeColumnDesc) {
            ExprNodeColumnDesc desc = (ExprNodeColumnDesc) expr;
            String fieldName = desc.getTabAlias() + "." + desc.getColumn();
            if (fieldName.indexOf("$$") < 0) {
                LogicalVariable var = getVariableOnly(fieldName);
                if (var == null) {
                    fieldName = "." + desc.getColumn();
                    var = getVariableOnly(fieldName);
                    if (var == null) {
                        fieldName = "null." + desc.getColumn();
                        var = getVariableOnly(fieldName);
                        if (var == null) {
                            throw new IllegalStateException(fieldName + " is wrong!!! ");
                        }
                    }
                }
                String name = this.logicalVariableToFieldMap.get(var);
                var = getVariableOnly(name);
                desc.setColumn(var.toString());
            }
        } else {
            if (expr.getChildren() != null && expr.getChildren().size() > 0) {
                List<ExprNodeDesc> children = expr.getChildren();
                for (ExprNodeDesc desc : children)
                    rewriteExpression(desc);
            }
        }
    }

    /**
     * rewrite an expression and substitute variables
     * 
     * @param expr
     *            hive expression
     */
    public void rewriteExpressionPartial(ExprNodeDesc expr) {
        if (expr instanceof ExprNodeColumnDesc) {
            ExprNodeColumnDesc desc = (ExprNodeColumnDesc) expr;
            String fieldName = desc.getTabAlias() + "." + desc.getColumn();
            if (fieldName.indexOf("$$") < 0) {
                LogicalVariable var = getVariableOnly(fieldName);
                desc.setColumn(var.toString());
            }
        } else {
            if (expr.getChildren() != null && expr.getChildren().size() > 0) {
                List<ExprNodeDesc> children = expr.getChildren();
                for (ExprNodeDesc desc : children)
                    rewriteExpressionPartial(desc);
            }
        }
    }

    // private void printOperatorSchema(Operator operator) {
    // // System.out.println(operator.getName());
    // // List<ColumnInfo> columns = operator.getSchema().getSignature();
    // // for (ColumnInfo column : columns) {
    // // System.out.print(column.getTabAlias() + "." +
    // // column.getInternalName() + "  ");
    // // }
    // // System.out.println();
    // }

    /**
     * translate scalar function expression
     * 
     * @param hiveExpr
     * @return
     */
    public Mutable<ILogicalExpression> translateScalarFucntion(ExprNodeDesc hiveExpr) {
        ILogicalExpression AlgebricksExpr;

        if (hiveExpr instanceof ExprNodeGenericFuncDesc) {
            List<Mutable<ILogicalExpression>> arguments = new ArrayList<Mutable<ILogicalExpression>>();
            List<ExprNodeDesc> children = hiveExpr.getChildren();

            for (ExprNodeDesc child : children)
                arguments.add(translateScalarFucntion(child));

            ExprNodeGenericFuncDesc funcExpr = (ExprNodeGenericFuncDesc) hiveExpr;
            GenericUDF genericUdf = funcExpr.getGenericUDF();
            UDF udf = null;
            if (genericUdf instanceof GenericUDFBridge) {
                GenericUDFBridge bridge = (GenericUDFBridge) genericUdf;
                try {
                    udf = bridge.getUdfClass().newInstance();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            /**
             * set up the hive function
             */
            Object hiveFunction = genericUdf;
            if (udf != null)
                hiveFunction = udf;

            FunctionIdentifier funcId = HiveAlgebricksBuiltInFunctionMap.INSTANCE.getAlgebricksFunctionId(hiveFunction
                    .getClass());
            if (funcId == null) {
                funcId = new FunctionIdentifier(ExpressionConstant.NAMESPACE, hiveFunction.getClass().getName());
            }

            Object functionInfo = null;
            if (genericUdf instanceof GenericUDFBridge) {
                functionInfo = funcExpr;
            }

            /**
             * generate the function call expression
             */
            ScalarFunctionCallExpression AlgebricksFuncExpr = new ScalarFunctionCallExpression(new HiveFunctionInfo(
                    funcId, functionInfo), arguments);
            AlgebricksExpr = AlgebricksFuncExpr;

        } else if (hiveExpr instanceof ExprNodeColumnDesc) {
            ExprNodeColumnDesc column = (ExprNodeColumnDesc) hiveExpr;
            LogicalVariable var = this.getVariable(column.getColumn());
            AlgebricksExpr = new VariableReferenceExpression(var);

        } else if (hiveExpr instanceof ExprNodeFieldDesc) {
            FunctionIdentifier funcId;
            funcId = new FunctionIdentifier(ExpressionConstant.NAMESPACE, ExpressionConstant.FIELDACCESS);

            ScalarFunctionCallExpression AlgebricksFuncExpr = new ScalarFunctionCallExpression(new HiveFunctionInfo(
                    funcId, hiveExpr));
            AlgebricksExpr = AlgebricksFuncExpr;
        } else if (hiveExpr instanceof ExprNodeConstantDesc) {
            ExprNodeConstantDesc hiveConst = (ExprNodeConstantDesc) hiveExpr;
            Object value = hiveConst.getValue();
            AlgebricksExpr = new ConstantExpression(new HivesterixConstantValue(value));
        } else if (hiveExpr instanceof ExprNodeNullDesc) {
            FunctionIdentifier funcId;
            funcId = new FunctionIdentifier(ExpressionConstant.NAMESPACE, ExpressionConstant.NULL);

            ScalarFunctionCallExpression AlgebricksFuncExpr = new ScalarFunctionCallExpression(new HiveFunctionInfo(
                    funcId, hiveExpr));

            AlgebricksExpr = AlgebricksFuncExpr;
        } else {
            throw new IllegalStateException("unknown hive expression");
        }
        return new MutableObject<ILogicalExpression>(AlgebricksExpr);
    }

    /**
     * translate aggregation function expression
     * 
     * @param aggregateDesc
     * @return
     */
    public Mutable<ILogicalExpression> translateAggregation(AggregationDesc aggregateDesc) {

        String UDAFName = aggregateDesc.getGenericUDAFName();

        List<Mutable<ILogicalExpression>> arguments = new ArrayList<Mutable<ILogicalExpression>>();
        List<ExprNodeDesc> children = aggregateDesc.getParameters();

        for (ExprNodeDesc child : children)
            arguments.add(translateScalarFucntion(child));

        FunctionIdentifier funcId = new FunctionIdentifier(ExpressionConstant.NAMESPACE, UDAFName + "("
                + aggregateDesc.getMode() + ")");
        HiveFunctionInfo funcInfo = new HiveFunctionInfo(funcId, aggregateDesc);
        AggregateFunctionCallExpression aggregationExpression = new AggregateFunctionCallExpression(funcInfo, false,
                arguments);
        return new MutableObject<ILogicalExpression>(aggregationExpression);
    }

    /**
     * translate aggregation function expression
     * 
     * @param aggregator
     * @return
     */
    public Mutable<ILogicalExpression> translateUnnestFunction(UDTFDesc udtfDesc, Mutable<ILogicalExpression> argument) {

        String UDTFName = udtfDesc.getUDTFName();

        FunctionIdentifier funcId = new FunctionIdentifier(ExpressionConstant.NAMESPACE, UDTFName);
        UnnestingFunctionCallExpression unnestingExpression = new UnnestingFunctionCallExpression(new HiveFunctionInfo(
                funcId, udtfDesc));
        unnestingExpression.getArguments().add(argument);
        return new MutableObject<ILogicalExpression>(unnestingExpression);
    }

    /**
     * get typeinfo
     */
    @Override
    public TypeInfo getType(LogicalVariable var) {
        return variableToType.get(var);
    }

    /**
     * get variable from variable name
     */
    @Override
    public LogicalVariable getVariable(String name) {
        return nameToLogicalVariableMap.get(name);
    }

    @Override
    public LogicalVariable getVariableFromFieldName(String fieldName) {
        return this.getVariableOnly(fieldName);
    }

    /**
     * set the metadata provider
     */
    @Override
    public void setMetadataProvider(IMetadataProvider<PartitionDesc, Object> metadata) {
        this.metaData = metadata;
    }

    /**
     * insert ReplicateOperator when necessary
     */
    private void insertReplicateOperator(List<Mutable<ILogicalOperator>> roots) {
        Map<Mutable<ILogicalOperator>, List<Mutable<ILogicalOperator>>> childToParentsMap = new HashMap<Mutable<ILogicalOperator>, List<Mutable<ILogicalOperator>>>();
        buildChildToParentsMapping(roots, childToParentsMap);
        for (Entry<Mutable<ILogicalOperator>, List<Mutable<ILogicalOperator>>> entry : childToParentsMap.entrySet()) {
            List<Mutable<ILogicalOperator>> pList = entry.getValue();
            if (pList.size() > 1) {
                ILogicalOperator rop = new ReplicateOperator(pList.size());
                Mutable<ILogicalOperator> ropRef = new MutableObject<ILogicalOperator>(rop);
                Mutable<ILogicalOperator> childRef = entry.getKey();
                rop.getInputs().add(childRef);
                for (Mutable<ILogicalOperator> parentRef : pList) {
                    ILogicalOperator parentOp = parentRef.getValue();
                    int index = parentOp.getInputs().indexOf(childRef);
                    parentOp.getInputs().set(index, ropRef);
                }
            }
        }
    }

    /**
     * build the mapping from child to Parents
     * 
     * @param roots
     * @param childToParentsMap
     */
    private void buildChildToParentsMapping(List<Mutable<ILogicalOperator>> roots,
            Map<Mutable<ILogicalOperator>, List<Mutable<ILogicalOperator>>> map) {
        for (Mutable<ILogicalOperator> opRef : roots) {
            List<Mutable<ILogicalOperator>> childRefs = opRef.getValue().getInputs();
            for (Mutable<ILogicalOperator> childRef : childRefs) {
                List<Mutable<ILogicalOperator>> parentList = map.get(childRef);
                if (parentList == null) {
                    parentList = new ArrayList<Mutable<ILogicalOperator>>();
                    map.put(childRef, parentList);
                }
                if (!parentList.contains(opRef))
                    parentList.add(opRef);
            }
            buildChildToParentsMapping(childRefs, map);
        }
    }
}
