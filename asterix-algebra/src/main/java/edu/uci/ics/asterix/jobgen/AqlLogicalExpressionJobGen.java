package edu.uci.ics.asterix.jobgen;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.formats.base.IDataFormat;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.runtime.base.IAggregateFunctionDynamicDescriptor;
import edu.uci.ics.asterix.runtime.base.IRunningAggregateFunctionDynamicDescriptor;
import edu.uci.ics.asterix.runtime.base.IScalarFunctionDynamicDescriptor;
import edu.uci.ics.asterix.runtime.base.ISerializableAggregateFunctionDynamicDescriptor;
import edu.uci.ics.asterix.runtime.base.IUnnestingFunctionDynamicDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.comparisons.ComparisonEvalFactory;
import edu.uci.ics.asterix.runtime.formats.FormatUtils;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression.FunctionKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ILogicalExpressionJobGen;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.ComparisonKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyRunningAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopySerializableAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyUnnestingFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;

public class AqlLogicalExpressionJobGen implements ILogicalExpressionJobGen {

    public static final AqlLogicalExpressionJobGen INSTANCE = new AqlLogicalExpressionJobGen();

    private AqlLogicalExpressionJobGen() {
    }

    @Override
    public ICopyAggregateFunctionFactory createAggregateFunctionFactory(AggregateFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
            throws AlgebricksException {
        ICopyEvaluatorFactory[] args = codegenArguments(expr, env, inputSchemas, context);
        IFunctionDescriptor fd;
        AqlMetadataProvider mp = (AqlMetadataProvider) context.getMetadataProvider();
        AqlCompiledMetadataDeclarations compiledDecls = mp.getMetadataDeclarations();
        try {
            fd = compiledDecls.getFormat().resolveFunction(expr, env);
        } catch (AlgebricksException e) {
            throw new AlgebricksException(e);
        }
        switch (fd.getFunctionDescriptorTag()) {
            case SCALAR: {
                throw new AlgebricksException(
                        "Trying to create an aggregate from a scalar evaluator function descriptor. (fi="
                                + expr.getFunctionIdentifier() + ")");
            }
            case AGGREGATE: {
                IAggregateFunctionDynamicDescriptor afdd = (IAggregateFunctionDynamicDescriptor) fd;
                return afdd.createAggregateFunctionFactory(args);
            }
            case SERIALAGGREGATE: {
                // temporal hack
                return null;
            }
            case RUNNINGAGGREGATE: {
                throw new AlgebricksException(
                        "Trying to create an aggregate from a running aggregate function descriptor.");
            }
            case UNNEST: {
                throw new AlgebricksException(
                        "Trying to create an aggregate from an unnesting aggregate function descriptor.");
            }

            default: {
                throw new IllegalStateException(fd.getFunctionDescriptorTag().toString());
            }
        }

    }

    @Override
    public ICopyRunningAggregateFunctionFactory createRunningAggregateFunctionFactory(StatefulFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
            throws AlgebricksException {
        ICopyEvaluatorFactory[] args = codegenArguments(expr, env, inputSchemas, context);
        IFunctionDescriptor fd;
        AqlMetadataProvider mp = (AqlMetadataProvider) context.getMetadataProvider();
        AqlCompiledMetadataDeclarations compiledDecls = mp.getMetadataDeclarations();
        try {
            fd = compiledDecls.getFormat().resolveFunction(expr, env);
        } catch (AlgebricksException e) {
            throw new AlgebricksException(e);
        }
        switch (fd.getFunctionDescriptorTag()) {
            case SCALAR: {
                throw new AlgebricksException(
                        "Trying to create a running aggregate from a scalar evaluator function descriptor. (fi="
                                + expr.getFunctionIdentifier() + ")");
            }
            case AGGREGATE: {
                throw new AlgebricksException(
                        "Trying to create a running aggregate from an aggregate function descriptor.");
            }
            case UNNEST: {
                throw new AlgebricksException(
                        "Trying to create a running aggregate from an unnesting function descriptor.");
            }
            case RUNNINGAGGREGATE: {
                IRunningAggregateFunctionDynamicDescriptor rafdd = (IRunningAggregateFunctionDynamicDescriptor) fd;
                return rafdd.createRunningAggregateFunctionFactory(args);
            }
            default: {
                throw new IllegalStateException();
            }
        }
    }

    @Override
    public ICopyUnnestingFunctionFactory createUnnestingFunctionFactory(UnnestingFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
            throws AlgebricksException {
        ICopyEvaluatorFactory[] args = codegenArguments(expr, env, inputSchemas, context);
        IFunctionDescriptor fd;
        AqlMetadataProvider mp = (AqlMetadataProvider) context.getMetadataProvider();
        AqlCompiledMetadataDeclarations compiledDecls = mp.getMetadataDeclarations();
        try {
            fd = compiledDecls.getFormat().resolveFunction(expr, env);
        } catch (AlgebricksException e) {
            throw new AlgebricksException(e);
        }
        switch (fd.getFunctionDescriptorTag()) {
            case UNNEST: {
                IUnnestingFunctionDynamicDescriptor ufdd = (IUnnestingFunctionDynamicDescriptor) fd;
                return ufdd.createUnnestingFunctionFactory(args);
            }
            default: {
                throw new AlgebricksException("Trying to create an unnesting function descriptor from a "
                        + fd.getFunctionDescriptorTag() + ". (fid=" + expr.getFunctionIdentifier() + ")");
            }
        }
    }

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(ILogicalExpression expr, IVariableTypeEnvironment env,
            IOperatorSchema[] inputSchemas, JobGenContext context) throws AlgebricksException {
        switch (expr.getExpressionTag()) {
            case VARIABLE: {
                VariableReferenceExpression v = (VariableReferenceExpression) expr;
                return createVariableEvaluatorFactory(v, inputSchemas, context);
            }
            case CONSTANT: {
                ConstantExpression c = (ConstantExpression) expr;
                return createConstantEvaluatorFactory(c, inputSchemas, context);
            }
            case FUNCTION_CALL: {
                AbstractFunctionCallExpression fun = (AbstractFunctionCallExpression) expr;
                if (fun.getKind() == FunctionKind.SCALAR) {
                    ScalarFunctionCallExpression scalar = (ScalarFunctionCallExpression) fun;
                    return createScalarFunctionEvaluatorFactory(scalar, env, inputSchemas, context);
                } else {
                    throw new AlgebricksException("Cannot create evaluator for function " + fun + " of kind "
                            + fun.getKind());
                }
            }
            default: {
                throw new IllegalStateException();
            }
        }
    }

    private ICopyEvaluatorFactory createVariableEvaluatorFactory(VariableReferenceExpression expr,
            IOperatorSchema[] inputSchemas, JobGenContext context) throws AlgebricksException {
        LogicalVariable variable = expr.getVariableReference();
        for (IOperatorSchema scm : inputSchemas) {
            int pos = scm.findVariable(variable);
            if (pos >= 0) {
                return new ColumnAccessEvalFactory(pos);
            }
        }
        throw new AlgebricksException("Variable " + variable + " could not be found in any input schema.");
    }

    private ICopyEvaluatorFactory createScalarFunctionEvaluatorFactory(AbstractFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
            throws AlgebricksException {
        ICopyEvaluatorFactory[] args = codegenArguments(expr, env, inputSchemas, context);
        FunctionIdentifier fi = expr.getFunctionIdentifier();
        ComparisonKind ck = AlgebricksBuiltinFunctions.getComparisonType(fi);
        if (ck != null) {
            return new ComparisonEvalFactory(args[0], args[1], ck);
        }

        IFunctionDescriptor fd;

        AqlMetadataProvider mp = (AqlMetadataProvider) context.getMetadataProvider();
        IDataFormat format = mp == null ? FormatUtils.getDefaultFormat() : mp.getMetadataDeclarations().getFormat();
        try {
            fd = format.resolveFunction(expr, env);
        } catch (AlgebricksException e) {
            throw new AlgebricksException(e);
        }

        switch (fd.getFunctionDescriptorTag()) {
            case SCALAR: {
                IScalarFunctionDynamicDescriptor sfdd = (IScalarFunctionDynamicDescriptor) fd;
                return sfdd.createEvaluatorFactory(args);
            }
            default: {
                throw new AlgebricksException("Trying to create a scalar function descriptor from a "
                        + fd.getFunctionDescriptorTag() + ". (fid=" + fi + ")");
            }
        }

    }

    private ICopyEvaluatorFactory createConstantEvaluatorFactory(ConstantExpression expr, IOperatorSchema[] inputSchemas,
            JobGenContext context) throws AlgebricksException {
        AqlMetadataProvider mp = (AqlMetadataProvider) context.getMetadataProvider();
        IDataFormat format = mp == null ? FormatUtils.getDefaultFormat() : mp.getMetadataDeclarations().getFormat();
        return format.getConstantEvalFactory(expr.getValue());
    }

    private ICopyEvaluatorFactory[] codegenArguments(AbstractFunctionCallExpression expr, IVariableTypeEnvironment env,
            IOperatorSchema[] inputSchemas, JobGenContext context) throws AlgebricksException {
        List<Mutable<ILogicalExpression>> arguments = expr.getArguments();
        int n = arguments.size();
        ICopyEvaluatorFactory[] args = new ICopyEvaluatorFactory[n];
        int i = 0;
        for (Mutable<ILogicalExpression> a : arguments) {
            args[i++] = createEvaluatorFactory(a.getValue(), env, inputSchemas, context);
        }
        return args;
    }

    @Override
    public ICopySerializableAggregateFunctionFactory createSerializableAggregateFunctionFactory(
            AggregateFunctionCallExpression expr, IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas,
            JobGenContext context) throws AlgebricksException {
        ICopyEvaluatorFactory[] args = codegenArguments(expr, env, inputSchemas, context);
        IFunctionDescriptor fd;
        AqlMetadataProvider mp = (AqlMetadataProvider) context.getMetadataProvider();
        AqlCompiledMetadataDeclarations compiledDecls = mp.getMetadataDeclarations();
        try {
            fd = compiledDecls.getFormat().resolveFunction(expr, env);
        } catch (AlgebricksException e) {
            throw new AlgebricksException(e);
        }
        switch (fd.getFunctionDescriptorTag()) {
            case SCALAR: {
                throw new AlgebricksException(
                        "Trying to create an aggregate from a scalar evaluator function descriptor. (fi="
                                + expr.getFunctionIdentifier() + ")");
            }
            case AGGREGATE: {
                if (AsterixBuiltinFunctions.isAggregateFunctionSerializable(fd.getIdentifier())) {
                    AggregateFunctionCallExpression serialAggExpr = AsterixBuiltinFunctions
                            .makeSerializableAggregateFunctionExpression(fd.getIdentifier(), expr.getArguments());
                    ISerializableAggregateFunctionDynamicDescriptor afdd = (ISerializableAggregateFunctionDynamicDescriptor) compiledDecls
                            .getFormat().resolveFunction(serialAggExpr, env);
                    return afdd.createAggregateFunctionFactory(args);
                } else {
                    throw new AlgebricksException(
                            "Trying to create a serializable aggregate from a non-serializable aggregate function descriptor. (fi="
                                    + expr.getFunctionIdentifier() + ")");
                }
            }
            case SERIALAGGREGATE: {
                ISerializableAggregateFunctionDynamicDescriptor afdd = (ISerializableAggregateFunctionDynamicDescriptor) fd;
                return afdd.createAggregateFunctionFactory(args);
            }
            case RUNNINGAGGREGATE: {
                throw new AlgebricksException(
                        "Trying to create an aggregate from a running aggregate function descriptor.");
            }
            case UNNEST: {
                throw new AlgebricksException(
                        "Trying to create an aggregate from an unnesting aggregate function descriptor.");
            }

            default: {
                throw new IllegalStateException();
            }
        }
    }

}
