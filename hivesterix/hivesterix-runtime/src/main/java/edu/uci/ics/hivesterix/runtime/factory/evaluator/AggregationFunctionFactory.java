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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import edu.uci.ics.hivesterix.logical.expression.ExpressionTranslator;
import edu.uci.ics.hivesterix.runtime.evaluator.AggregationFunctionEvaluator;
import edu.uci.ics.hivesterix.runtime.jobgen.Schema;
import edu.uci.ics.hivesterix.serde.lazy.LazyFactory;
import edu.uci.ics.hivesterix.serde.lazy.LazyObject;
import edu.uci.ics.hivesterix.serde.lazy.LazySerDe;
import edu.uci.ics.hivesterix.serde.lazy.LazyUtils;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunctionFactory;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;

@SuppressWarnings("deprecation")
public class AggregationFunctionFactory implements ICopyAggregateFunctionFactory {

    private static final long serialVersionUID = 1L;

    /**
     * list of parameters' serialization
     */
    private List<String> parametersSerialization = new ArrayList<String>();

    /**
     * the name of the udf
     */
    private String genericUDAFName;

    /**
     * aggregation mode
     */
    private GenericUDAFEvaluator.Mode mode;

    /**
     * list of type info
     */
    private List<TypeInfo> types = new ArrayList<TypeInfo>();

    /**
     * distinct or not
     */
    private boolean distinct;

    /**
     * the schema of incoming rows
     */
    private Schema rowSchema;

    /**
     * list of parameters
     */
    private transient List<ExprNodeDesc> parametersOrigin;

    /**
     * row inspector
     */
    private transient ObjectInspector rowInspector = null;

    /**
     * output object inspector
     */
    private transient ObjectInspector outputInspector = null;

    /**
     * output object inspector
     */
    private transient ObjectInspector outputInspectorPartial = null;

    /**
     * parameter inspectors
     */
    private transient ObjectInspector[] parameterInspectors = null;

    /**
     * expression desc
     */
    private transient HashMap<Long, List<ExprNodeDesc>> parameterExprs = new HashMap<Long, List<ExprNodeDesc>>();

    /**
     * evaluators
     */
    private transient HashMap<Long, ExprNodeEvaluator[]> evaluators = new HashMap<Long, ExprNodeEvaluator[]>();

    /**
     * cached parameter objects
     */
    private transient HashMap<Long, Object[]> cachedParameters = new HashMap<Long, Object[]>();

    /**
     * cached row object: one per thread
     */
    private transient HashMap<Long, LazyObject<? extends ObjectInspector>> cachedRowObjects = new HashMap<Long, LazyObject<? extends ObjectInspector>>();

    /**
     * we only use lazy serde to do serialization
     */
    private transient HashMap<Long, SerDe> serDe = new HashMap<Long, SerDe>();

    /**
     * udaf evaluators
     */
    private transient HashMap<Long, GenericUDAFEvaluator> udafsPartial = new HashMap<Long, GenericUDAFEvaluator>();

    /**
     * udaf evaluators
     */
    private transient HashMap<Long, GenericUDAFEvaluator> udafsComplete = new HashMap<Long, GenericUDAFEvaluator>();

    /**
     * aggregation function desc
     */
    private transient AggregationDesc aggregator;

    /**
     * @param aggregator
     *            Algebricks function call expression
     * @param oi
     *            schema
     */
    public AggregationFunctionFactory(AggregateFunctionCallExpression expression, Schema oi,
            IVariableTypeEnvironment env) throws AlgebricksException {

        try {
            aggregator = (AggregationDesc) ExpressionTranslator.getHiveExpression(expression, env);
        } catch (Exception e) {
            e.printStackTrace();
            throw new AlgebricksException(e.getMessage());
        }
        init(aggregator.getParameters(), aggregator.getGenericUDAFName(), aggregator.getMode(),
                aggregator.getDistinct(), oi);
    }

    /**
     * constructor of aggregation function factory
     * 
     * @param inputs
     * @param name
     * @param udafMode
     * @param distinct
     * @param oi
     */
    private void init(List<ExprNodeDesc> inputs, String name, GenericUDAFEvaluator.Mode udafMode, boolean distinct,
            Schema oi) {
        parametersOrigin = inputs;
        genericUDAFName = name;
        mode = udafMode;
        this.distinct = distinct;
        rowSchema = oi;

        for (ExprNodeDesc input : inputs) {
            TypeInfo type = input.getTypeInfo();
            if (type instanceof StructTypeInfo) {
                types.add(TypeInfoFactory.doubleTypeInfo);
            } else {
                types.add(type);
            }

            String s = Utilities.serializeExpression(input);
            parametersSerialization.add(s);
        }
    }

    @Override
    public synchronized ICopyAggregateFunction createAggregateFunction(IDataOutputProvider provider)
            throws AlgebricksException {
        /**
         * list of object inspectors correlated to types
         */
        List<ObjectInspector> oiListForTypes = new ArrayList<ObjectInspector>();
        for (TypeInfo type : types) {
            oiListForTypes.add(LazyUtils.getLazyObjectInspectorFromTypeInfo(type, false));
        }

        if (parametersOrigin == null) {
            Configuration config = new Configuration();
            config.setClassLoader(this.getClass().getClassLoader());
            /**
             * in case of class.forname(...) call in hive code
             */
            Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

            parametersOrigin = new ArrayList<ExprNodeDesc>();
            for (String serialization : parametersSerialization) {
                parametersOrigin.add(Utilities.deserializeExpression(serialization, config));
            }
        }

        /**
         * exprs
         */
        if (parameterExprs == null)
            parameterExprs = new HashMap<Long, List<ExprNodeDesc>>();

        /**
         * evaluators
         */
        if (evaluators == null)
            evaluators = new HashMap<Long, ExprNodeEvaluator[]>();

        /**
         * cached parameter objects
         */
        if (cachedParameters == null)
            cachedParameters = new HashMap<Long, Object[]>();

        /**
         * cached row object: one per thread
         */
        if (cachedRowObjects == null)
            cachedRowObjects = new HashMap<Long, LazyObject<? extends ObjectInspector>>();

        /**
         * we only use lazy serde to do serialization
         */
        if (serDe == null)
            serDe = new HashMap<Long, SerDe>();

        /**
         * UDAF functions
         */
        if (udafsComplete == null)
            udafsComplete = new HashMap<Long, GenericUDAFEvaluator>();

        /**
         * UDAF functions
         */
        if (udafsPartial == null)
            udafsPartial = new HashMap<Long, GenericUDAFEvaluator>();

        if (parameterInspectors == null)
            parameterInspectors = new ObjectInspector[parametersOrigin.size()];

        if (rowInspector == null)
            rowInspector = rowSchema.toObjectInspector();

        // get current thread id
        long threadId = Thread.currentThread().getId();

        /**
         * expressions, expressions are thread local
         */
        List<ExprNodeDesc> parameters = parameterExprs.get(threadId);
        if (parameters == null) {
            parameters = new ArrayList<ExprNodeDesc>();
            for (ExprNodeDesc parameter : parametersOrigin)
                parameters.add(parameter.clone());
            parameterExprs.put(threadId, parameters);
        }

        /**
         * cached parameter objects
         */
        Object[] cachedParas = cachedParameters.get(threadId);
        if (cachedParas == null) {
            cachedParas = new Object[parameters.size()];
            cachedParameters.put(threadId, cachedParas);
        }

        /**
         * cached row object: one per thread
         */
        LazyObject<? extends ObjectInspector> cachedRowObject = cachedRowObjects.get(threadId);
        if (cachedRowObject == null) {
            cachedRowObject = LazyFactory.createLazyObject(rowInspector);
            cachedRowObjects.put(threadId, cachedRowObject);
        }

        /**
         * we only use lazy serde to do serialization
         */
        SerDe lazySer = serDe.get(threadId);
        if (lazySer == null) {
            lazySer = new LazySerDe();
            serDe.put(threadId, lazySer);
        }

        /**
         * evaluators
         */
        ExprNodeEvaluator[] evals = evaluators.get(threadId);
        if (evals == null) {
            evals = new ExprNodeEvaluator[parameters.size()];
            evaluators.put(threadId, evals);
        }

        GenericUDAFEvaluator udafPartial;
        GenericUDAFEvaluator udafComplete;

        // initialize object inspectors
        try {
            /**
             * evaluators, udf, object inpsectors are shared in one thread
             */
            for (int i = 0; i < evals.length; i++) {
                if (evals[i] == null) {
                    evals[i] = ExprNodeEvaluatorFactory.get(parameters.get(i));
                    if (parameterInspectors[i] == null) {
                        parameterInspectors[i] = evals[i].initialize(rowInspector);
                    } else {
                        evals[i].initialize(rowInspector);
                    }
                }
            }

            udafComplete = udafsComplete.get(threadId);
            if (udafComplete == null) {
                try {
                    udafComplete = FunctionRegistry.getGenericUDAFEvaluator(genericUDAFName, oiListForTypes, distinct,
                            false);
                } catch (HiveException e) {
                    throw new AlgebricksException(e);
                }
                udafsComplete.put(threadId, udafComplete);
                udafComplete.init(mode, parameterInspectors);
            }

            // multiple stage group by, determined by the mode parameter
            if (outputInspector == null)
                outputInspector = udafComplete.init(mode, parameterInspectors);

            // initial partial gby udaf
            GenericUDAFEvaluator.Mode partialMode;
            // adjust mode for external groupby
            if (mode == GenericUDAFEvaluator.Mode.COMPLETE)
                partialMode = GenericUDAFEvaluator.Mode.PARTIAL1;
            else if (mode == GenericUDAFEvaluator.Mode.FINAL)
                partialMode = GenericUDAFEvaluator.Mode.PARTIAL2;
            else
                partialMode = mode;
            udafPartial = udafsPartial.get(threadId);
            if (udafPartial == null) {
                try {
                    udafPartial = FunctionRegistry.getGenericUDAFEvaluator(genericUDAFName, oiListForTypes, distinct,
                            false);
                } catch (HiveException e) {
                    throw new AlgebricksException(e);
                }
                udafPartial.init(partialMode, parameterInspectors);
                udafsPartial.put(threadId, udafPartial);
            }

            // multiple stage group by, determined by the mode parameter
            if (outputInspectorPartial == null)
                outputInspectorPartial = udafPartial.init(partialMode, parameterInspectors);
        } catch (Exception e) {
            e.printStackTrace();
            throw new AlgebricksException(e);
        }

        return new AggregationFunctionEvaluator(parameters, types, genericUDAFName, mode, distinct, rowInspector,
                provider.getDataOutput(), evals, parameterInspectors, cachedParas, lazySer, cachedRowObject,
                udafPartial, udafComplete, outputInspector, outputInspectorPartial);
    }

    public String toString() {
        return "aggregation function expression evaluator factory: " + this.genericUDAFName;
    }
}
