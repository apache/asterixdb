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
package edu.uci.ics.hivesterix.runtime.factory.evaluator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

import edu.uci.ics.hivesterix.logical.expression.ExpressionTranslator;
import edu.uci.ics.hivesterix.runtime.evaluator.FunctionExpressionEvaluator;
import edu.uci.ics.hivesterix.runtime.jobgen.Schema;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;

public class ScalarFunctionExpressionEvaluatorFactory implements ICopyEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private transient ExprNodeGenericFuncDesc expr;

    private String exprSerialization;

    private Schema inputSchema;

    private transient Configuration config;

    public ScalarFunctionExpressionEvaluatorFactory(ILogicalExpression expression, Schema schema,
            IVariableTypeEnvironment env) throws AlgebricksException {
        try {
            expr = (ExprNodeGenericFuncDesc) ExpressionTranslator.getHiveExpression(expression, env);

            exprSerialization = Utilities.serializeExpression(expr);

        } catch (Exception e) {
            e.printStackTrace();
            throw new AlgebricksException(e.getMessage());
        }
        inputSchema = schema;
    }

    public synchronized ICopyEvaluator createEvaluator(IDataOutputProvider output) throws AlgebricksException {
        if (expr == null) {
            configClassLoader();
            expr = (ExprNodeGenericFuncDesc) Utilities.deserializeExpression(exprSerialization, config);
        }

        ExprNodeGenericFuncDesc funcDesc = (ExprNodeGenericFuncDesc) expr.clone();
        return new FunctionExpressionEvaluator(funcDesc, inputSchema.toObjectInspector(), output);
    }

    private void configClassLoader() {
        config = new Configuration();
        ClassLoader loader = this.getClass().getClassLoader();
        config.setClassLoader(loader);
        Thread.currentThread().setContextClassLoader(loader);
    }

    public String toString() {
        if (expr == null) {
            configClassLoader();
            expr = (ExprNodeGenericFuncDesc) Utilities.deserializeExpression(exprSerialization, new Configuration());
        }

        return "function expression evaluator factory: " + expr.getExprString();
    }

}
