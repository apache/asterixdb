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
package org.apache.hyracks.algebricks.core.algebra.expressions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.properties.FunctionalDependency;

public abstract class AbstractFunctionCallExpression extends AbstractLogicalExpression {

    public enum FunctionKind {
        SCALAR,
        STATEFUL,
        AGGREGATE,
        UNNEST
    }

    protected IFunctionInfo finfo;
    final private List<Mutable<ILogicalExpression>> arguments;
    private Object[] opaqueParameters;
    private final FunctionKind kind;
    private final Map<Object, IExpressionAnnotation> annotationMap = new HashMap<Object, IExpressionAnnotation>();

    public AbstractFunctionCallExpression(FunctionKind kind, IFunctionInfo finfo,
            List<Mutable<ILogicalExpression>> arguments) {
        this.kind = kind;
        this.finfo = finfo;
        this.arguments = arguments;
    }

    public AbstractFunctionCallExpression(FunctionKind kind, IFunctionInfo finfo) {
        this.kind = kind;
        this.finfo = finfo;
        this.arguments = new ArrayList<Mutable<ILogicalExpression>>();
    }

    public AbstractFunctionCallExpression(FunctionKind kind, IFunctionInfo finfo,
            Mutable<ILogicalExpression>... expressions) {
        this(kind, finfo);
        for (Mutable<ILogicalExpression> e : expressions) {
            this.arguments.add(e);
        }
    }

    public void setOpaqueParameters(Object[] opaqueParameters) {
        this.opaqueParameters = opaqueParameters;
    }

    public Object[] getOpaqueParameters() {
        return opaqueParameters;
    }

    public FunctionKind getKind() {
        return kind;
    }

    protected List<Mutable<ILogicalExpression>> cloneArguments() {
        List<Mutable<ILogicalExpression>> clonedArgs = new ArrayList<Mutable<ILogicalExpression>>(arguments.size());
        for (Mutable<ILogicalExpression> e : arguments) {
            ILogicalExpression e2 = e.getValue().cloneExpression();
            clonedArgs.add(new MutableObject<ILogicalExpression>(e2));
        }
        return clonedArgs;
    }

    public FunctionIdentifier getFunctionIdentifier() {
        return finfo.getFunctionIdentifier();
    }

    public IFunctionInfo getFunctionInfo() {
        return finfo;
    }

    public void setFunctionInfo(IFunctionInfo finfo) {
        this.finfo = finfo;
    }

    public List<Mutable<ILogicalExpression>> getArguments() {
        return arguments;
    }

    @Override
    public String toString() {
        return finfo.display(arguments);
    }

    @Override
    public LogicalExpressionTag getExpressionTag() {
        return LogicalExpressionTag.FUNCTION_CALL;
    }

    @Override
    public void getUsedVariables(Collection<LogicalVariable> vars) {
        for (Mutable<ILogicalExpression> arg : arguments) {
            arg.getValue().getUsedVariables(vars);
        }
    }

    @Override
    public void substituteVar(LogicalVariable v1, LogicalVariable v2) {
        for (Mutable<ILogicalExpression> arg : arguments) {
            arg.getValue().substituteVar(v1, v2);
        }
    }

    @Override
    public void getConstraintsAndEquivClasses(Collection<FunctionalDependency> fds,
            Map<LogicalVariable, EquivalenceClass> equivClasses) {
        FunctionIdentifier funId = getFunctionIdentifier();
        if (funId.equals(AlgebricksBuiltinFunctions.AND)) {
            for (Mutable<ILogicalExpression> a : arguments) {
                a.getValue().getConstraintsAndEquivClasses(fds, equivClasses);
            }
        } else if (funId.equals(AlgebricksBuiltinFunctions.EQ)) {
            ILogicalExpression opLeft = arguments.get(0).getValue();
            ILogicalExpression opRight = arguments.get(1).getValue();
            if (opLeft.getExpressionTag() == LogicalExpressionTag.CONSTANT
                    && opRight.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                ConstantExpression op1 = (ConstantExpression) opLeft;
                VariableReferenceExpression op2 = (VariableReferenceExpression) opRight;
                getFDsAndEquivClassesForEqWithConstant(op1, op2, fds, equivClasses);
            } else if (opLeft.getExpressionTag() == LogicalExpressionTag.VARIABLE
                    && opRight.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                VariableReferenceExpression op1 = (VariableReferenceExpression) opLeft;
                VariableReferenceExpression op2 = (VariableReferenceExpression) opRight;
                getFDsAndEquivClassesForColumnEq(op1, op2, fds, equivClasses);
            }
        }
    }

    @Override
    public void getConstraintsForOuterJoin(Collection<FunctionalDependency> fds,
            Collection<LogicalVariable> outerVars) {
        FunctionIdentifier funId = getFunctionIdentifier();
        if (funId.equals(AlgebricksBuiltinFunctions.AND)) {
            for (Mutable<ILogicalExpression> a : arguments) {
                a.getValue().getConstraintsForOuterJoin(fds, outerVars);
            }
        } else if (funId.equals(AlgebricksBuiltinFunctions.EQ)) {
            ILogicalExpression opLeft = arguments.get(0).getValue();
            ILogicalExpression opRight = arguments.get(1).getValue();
            if (opLeft.getExpressionTag() == LogicalExpressionTag.VARIABLE
                    && opRight.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                LogicalVariable var1 = ((VariableReferenceExpression) opLeft).getVariableReference();
                LogicalVariable var2 = ((VariableReferenceExpression) opRight).getVariableReference();
                if (outerVars.contains(var1)) {
                    addFD(fds, var1, var2);
                }
                if (outerVars.contains(var2)) {
                    addFD(fds, var2, var1);
                }
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AbstractFunctionCallExpression)) {
            return false;
        } else {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) obj;
            boolean equal = getFunctionIdentifier().equals(fce.getFunctionIdentifier());
            if (!equal) {
                return false;
            }
            int argumentCount = arguments.size();
            List<Mutable<ILogicalExpression>> fceArguments = fce.getArguments();
            if (argumentCount != fceArguments.size()) {
                return false;
            }
            for (int i = 0; i < argumentCount; i++) {
                ILogicalExpression argument = arguments.get(i).getValue();
                ILogicalExpression fceArgument = fceArguments.get(i).getValue();
                if (!argument.equals(fceArgument)) {
                    return false;
                }
            }
            return Arrays.deepEquals(opaqueParameters, fce.opaqueParameters);
        }
    }

    @Override
    public int hashCode() {
        int h = finfo.hashCode();
        for (Mutable<ILogicalExpression> e : arguments) {
            h = h * 41 + e.getValue().hashCode();
        }
        if (opaqueParameters != null) {
            h = h * 31 + Arrays.deepHashCode(opaqueParameters);
        }
        return h;
    }

    @Override
    public boolean splitIntoConjuncts(List<Mutable<ILogicalExpression>> conjs) {
        if (!getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.AND) || arguments.size() <= 1) {
            return false;
        } else {
            conjs.addAll(arguments);
            return true;
        }
    }

    public Map<Object, IExpressionAnnotation> getAnnotations() {
        return annotationMap;
    }

    protected Map<Object, IExpressionAnnotation> cloneAnnotations() {
        Map<Object, IExpressionAnnotation> m = new HashMap<Object, IExpressionAnnotation>();
        for (Object k : annotationMap.keySet()) {
            IExpressionAnnotation annot2 = annotationMap.get(k).copy();
            m.put(k, annot2);
        }
        return m;
    }

    private final static void addFD(Collection<FunctionalDependency> fds, LogicalVariable var1, LogicalVariable var2) {
        LinkedList<LogicalVariable> set1 = new LinkedList<LogicalVariable>();
        set1.add(var1);
        LinkedList<LogicalVariable> set2 = new LinkedList<LogicalVariable>();
        set2.add(var2);
        FunctionalDependency fd1 = new FunctionalDependency(set1, set2);
        fds.add(fd1);
    }

    private final static void getFDsAndEquivClassesForEqWithConstant(ConstantExpression c,
            VariableReferenceExpression v, Collection<FunctionalDependency> fds,
            Map<LogicalVariable, EquivalenceClass> equivClasses) {
        LogicalVariable var = v.getVariableReference();
        LinkedList<LogicalVariable> head = new LinkedList<LogicalVariable>();
        // empty set in the head
        LinkedList<LogicalVariable> tail = new LinkedList<LogicalVariable>();
        tail.add(var);
        FunctionalDependency fd = new FunctionalDependency(head, tail);
        fds.add(fd);

        EquivalenceClass ec = equivClasses.get(var);
        if (ec == null) {
            LinkedList<LogicalVariable> members = new LinkedList<LogicalVariable>();
            members.add(var);
            EquivalenceClass eclass = new EquivalenceClass(members, c);
            equivClasses.put(var, eclass);
        } else {
            if (ec.representativeIsConst()) {
                ILogicalExpression c1 = ec.getConstRepresentative();
                if (!c1.equals(c)) {
                    // here I could also rewrite to FALSE
                    return;
                }
            }
            ec.setConstRepresentative(c);
        }
    }

    /*
     * Obs.: mgmt. of equiv. classes should use a more efficient data
     * structure,if we are to implem. cost-bazed optim.
     */
    private final static void getFDsAndEquivClassesForColumnEq(VariableReferenceExpression v1,
            VariableReferenceExpression v2, Collection<FunctionalDependency> fds,
            Map<LogicalVariable, EquivalenceClass> equivClasses) {
        LogicalVariable var1 = v1.getVariableReference();
        LogicalVariable var2 = v2.getVariableReference();
        LinkedList<LogicalVariable> set1 = new LinkedList<LogicalVariable>();
        set1.add(var1);
        LinkedList<LogicalVariable> set2 = new LinkedList<LogicalVariable>();
        set2.add(var2);
        FunctionalDependency fd1 = new FunctionalDependency(set1, set2);
        FunctionalDependency fd2 = new FunctionalDependency(set2, set1);
        fds.add(fd1);
        fds.add(fd2);

        EquivalenceClass ec1 = equivClasses.get(var1);
        EquivalenceClass ec2 = equivClasses.get(var2);
        if (ec1 == null && ec2 == null) {
            LinkedList<LogicalVariable> members = new LinkedList<LogicalVariable>();
            members.add(var1);
            members.add(var2);
            EquivalenceClass ec = new EquivalenceClass(members, var1);
            equivClasses.put(var1, ec);
            equivClasses.put(var2, ec);
        } else if (ec1 == null && ec2 != null) {
            ec2.addMember(var1);
            equivClasses.put(var1, ec2);
        } else if (ec2 == null && ec1 != null) {
            ec1.addMember(var2);
            equivClasses.put(var2, ec1);
        } else {
            ec1.merge(ec2);
            for (LogicalVariable w : equivClasses.keySet()) {
                if (ec2.getMembers().contains(w)) {
                    equivClasses.put(w, ec1);
                }
            }
        }
    }

    @Override
    public boolean isFunctional() {
        if (!finfo.isFunctional()) {
            return false;
        }

        for (Mutable<ILogicalExpression> e : arguments) {
            if (!e.getValue().isFunctional()) {
                return false;
            }
        }
        return true;
    }

}
