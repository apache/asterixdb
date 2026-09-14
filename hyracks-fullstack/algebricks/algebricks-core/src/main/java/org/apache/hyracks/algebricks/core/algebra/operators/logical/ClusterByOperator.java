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
package org.apache.hyracks.algebricks.core.algebra.operators.logical;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.typing.NonPropagatingTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * CLUSTER BY as the query expressed it. Consumes the block's rows and emits one tuple per cluster: its id,
 * its centroid and its members. Blocking; non-propagating, like GROUP BY.
 * <p>
 * The node says <em>what</em> to compute (its {@code ClusterByOptions}) and has no physical operator of its
 * own. Every logical rule sees one opaque node over one ordinary input, and a rule at the head of the
 * physical phase expands it into the stages that implement the algorithm, the way the combiner rules expand
 * one group-by into a local and a global one.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_CLI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Nested-plans spike: members carried as a GROUP BY-style nested plan")
public class ClusterByOperator extends AbstractOperatorWithNestedPlans {

    // The input's vector-valued variable, held as an expression so substitution and pruning rules see it.
    private final Mutable<ILogicalExpression> vectorRef;
    // One member's record, the row as CLUSTER AS sees it. The translator builds it and listifies it in this
    // operator's nested plan, where the aggregate pushdown can rewrite it before the expansion adopts it.
    private Mutable<ILogicalExpression> memberRecordRef;
    // The output, one tuple per cluster: id, centroid and members. Types are opaque Objects supplied by the
    // translator, since the type system lives above Algebricks.
    private LogicalVariable clusterIdVar;
    private LogicalVariable centroidVar;
    private LogicalVariable membersVar;
    // The per-row assignment centroid, bound below the operator; the expansion redefines it as the row's
    // entry in the final centroid list, and the reported centroid aggregates it.
    private LogicalVariable assignedCentroidVar;
    private final Object clusterIdVarType;
    private final Object centroidVarType;
    private final Object membersVarType;
    // The validated options, immutable and shared with the language clause that carried them.
    private final Object options;
    // Types the members list from the member record once the input is typed, since a list type is an
    // Asterix notion the operator cannot form itself.
    private IMembersTypeComputer membersTypeComputer;
    // Decorations, as GROUP BY carries them: each variable is bound above the operator to the value its
    // expression has below it. The expansion hands them to its labelling GROUP BY.
    private final List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> decorList = new ArrayList<>();

    /** How the members list is typed from one member's record type; implemented above Algebricks. */
    @FunctionalInterface
    public interface IMembersTypeComputer {
        Object membersType(ILogicalExpression memberRecord, IVariableTypeEnvironment inputEnv, ITypingContext ctx)
                throws AlgebricksException;
    }

    public ClusterByOperator(Mutable<ILogicalExpression> vectorRef, LogicalVariable clusterIdVar,
            Object clusterIdVarType, LogicalVariable centroidVar, Object centroidVarType, LogicalVariable membersVar,
            Object membersVarType, Object options) {
        this.vectorRef = vectorRef;
        this.clusterIdVar = clusterIdVar;
        this.clusterIdVarType = clusterIdVarType;
        this.centroidVar = centroidVar;
        this.centroidVarType = centroidVarType;
        this.membersVar = membersVar;
        this.membersVarType = membersVarType;
        this.options = options;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.CLUSTER_BY;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitClusterByOperator(this, arg);
    }

    @Override
    public boolean isMap() {
        // Blocking: the input is consumed whole before any cluster is emitted.
        return false;
    }

    @Override
    public void recomputeSchema() {
        // A grouping operator: the input tuples are consumed and one tuple per cluster comes out.
        schema = new ArrayList<>();
        schema.add(clusterIdVar);
        schema.add(centroidVar);
        if (nestedPlans.isEmpty()) {
            schema.add(membersVar);
        } else {
            for (ILogicalPlan np : nestedPlans) {
                for (Mutable<ILogicalOperator> r : np.getRoots()) {
                    schema.addAll(r.getValue().getSchema());
                }
            }
        }
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : decorList) {
            schema.add(p.first);
        }
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return new VariablePropagationPolicy() {
            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources)
                    throws AlgebricksException {
                target.addVariable(clusterIdVar);
                target.addVariable(centroidVar);
                if (nestedPlans.isEmpty()) {
                    target.addVariable(membersVar);
                } else {
                    for (ILogicalPlan np : nestedPlans) {
                        for (Mutable<ILogicalOperator> r : np.getRoots()) {
                            for (LogicalVariable v : r.getValue().getSchema()) {
                                target.addVariable(v);
                            }
                        }
                    }
                }
                for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : decorList) {
                    target.addVariable(p.first);
                }
            }
        };
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        // memberRecordRef is bound after construction (setMemberRecordRef), so a rule can reach this
        // operator before it exists.
        boolean changed = vectorRef != null && visitor.transform(vectorRef);
        changed |= memberRecordRef != null && visitor.transform(memberRecordRef);
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : decorList) {
            changed |= visitor.transform(p.second);
        }
        return changed;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        // Non-propagating, agreeing with recomputeSchema and the propagation policy, since the input tuples
        // are consumed and only the cluster variables are live downstream; GroupByOperator has the same shape.
        IVariableTypeEnvironment env;
        if (nestedPlans.isEmpty()) {
            env = new NonPropagatingTypeEnvironment(ctx.getExpressionTypeComputer(), ctx.getMetadataProvider());
            env.setVarType(membersVar, membersType(ctx));
        } else {
            // The nested plans' root aggregates type members (and any aggregate a rule rewrote the listify
            // into) exactly as GROUP BY's nested plans do; the operator's own outputs are set on top.
            env = createNestedPlansPropagatingTypeEnvironment(ctx, false);
        }
        env.setVarType(clusterIdVar, clusterIdVarType);
        env.setVarType(centroidVar, centroidVarType);
        if (!decorList.isEmpty() && !inputs.isEmpty()) {
            IVariableTypeEnvironment inputEnv = ctx.getOutputTypeEnvironment(inputs.get(0).getValue());
            for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : decorList) {
                env.setVarType(p.first, inputEnv.getType(p.second.getValue()));
            }
        }
        return env;
    }

    private Object membersType(ITypingContext ctx) throws AlgebricksException {
        if (membersTypeComputer == null || memberRecordRef == null || inputs.isEmpty()) {
            return membersVarType;
        }
        IVariableTypeEnvironment inputEnv = ctx.getOutputTypeEnvironment(inputs.get(0).getValue());
        return inputEnv == null ? membersVarType
                : membersTypeComputer.membersType(memberRecordRef.getValue(), inputEnv, ctx);
    }

    /** The input's vector-valued variable. */
    public LogicalVariable getVectorVariable() {
        return vectorRef == null ? null : ((VariableReferenceExpression) vectorRef.getValue()).getVariableReference();
    }

    public Mutable<ILogicalExpression> getVectorRef() {
        return vectorRef;
    }

    public Mutable<ILogicalExpression> getMemberRecordRef() {
        return memberRecordRef;
    }

    public void setMemberRecordRef(Mutable<ILogicalExpression> memberRecordRef) {
        this.memberRecordRef = memberRecordRef;
    }

    public IMembersTypeComputer getMembersTypeComputer() {
        return membersTypeComputer;
    }

    public void setMembersTypeComputer(IMembersTypeComputer membersTypeComputer) {
        this.membersTypeComputer = membersTypeComputer;
    }

    /** The member-record variable, or null before the translator has set it. */
    public LogicalVariable getMemberRecordVariable() {
        return memberRecordRef == null ? null
                : ((VariableReferenceExpression) memberRecordRef.getValue()).getVariableReference();
    }

    public LogicalVariable getClusterIdVariable() {
        return clusterIdVar;
    }

    public LogicalVariable getCentroidVariable() {
        return centroidVar;
    }

    public LogicalVariable getMembersVariable() {
        return membersVar;
    }

    public Object getClusterIdVarType() {
        return clusterIdVarType;
    }

    public Object getCentroidVarType() {
        return centroidVarType;
    }

    public Object getMembersVarType() {
        return membersVarType;
    }

    public void setClusterIdVariable(LogicalVariable v) {
        this.clusterIdVar = v;
    }

    public void setCentroidVariable(LogicalVariable v) {
        this.centroidVar = v;
    }

    public void setMembersVariable(LogicalVariable v) {
        this.membersVar = v;
    }

    public LogicalVariable getAssignedCentroidVariable() {
        return assignedCentroidVar;
    }

    public void setAssignedCentroidVariable(LogicalVariable v) {
        this.assignedCentroidVar = v;
    }

    /** The validated options, whole; equality on it is what makes two clusterings the same computation. */
    public Object getOptions() {
        return options;
    }

    public List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> getDecorList() {
        return decorList;
    }

    public void addDecorExpression(LogicalVariable variable, ILogicalExpression expression) {
        decorList.add(new Pair<>(variable, new MutableObject<>(expression)));
    }

    /** The decoration variables, in list order. */
    public List<LogicalVariable> getDecorVariables() {
        List<LogicalVariable> vars = new ArrayList<>(decorList.size());
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : decorList) {
            vars.add(p.first);
        }
        return vars;
    }

    @Override
    public void getUsedVariablesExceptNestedPlans(Collection<LogicalVariable> vars) {
        if (vectorRef != null) {
            vectorRef.getValue().getUsedVariables(vars);
        }
        if (memberRecordRef != null) {
            memberRecordRef.getValue().getUsedVariables(vars);
        }
        if (assignedCentroidVar != null) {
            vars.add(assignedCentroidVar);
        }
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : decorList) {
            p.second.getValue().getUsedVariables(vars);
        }
    }

    @Override
    public void getProducedVariablesExceptNestedPlans(Collection<LogicalVariable> vars) {
        vars.add(clusterIdVar);
        vars.add(centroidVar);
        if (nestedPlans.isEmpty()) {
            vars.add(membersVar);
        }
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : decorList) {
            vars.add(p.first);
        }
    }

}
