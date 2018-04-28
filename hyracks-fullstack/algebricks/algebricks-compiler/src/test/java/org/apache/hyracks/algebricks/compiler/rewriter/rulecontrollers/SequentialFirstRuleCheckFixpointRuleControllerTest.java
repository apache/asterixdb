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

package org.apache.hyracks.algebricks.compiler.rewriter.rulecontrollers;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest({ SequentialFirstRuleCheckFixpointRuleController.class, AbstractLogicalOperator.class })
public class SequentialFirstRuleCheckFixpointRuleControllerTest {

    @Test
    public void testRewriteWithRuleCollection() throws Exception {
        SequentialFirstRuleCheckFixpointRuleController ruleController =
                new SequentialFirstRuleCheckFixpointRuleController(true);

        // Specifies three rules - R1, R2, and R3.
        IAlgebraicRewriteRule firstRule = mock(IAlgebraicRewriteRule.class);
        IAlgebraicRewriteRule secondRule = mock(IAlgebraicRewriteRule.class);
        IAlgebraicRewriteRule thirdRule = mock(IAlgebraicRewriteRule.class);
        List<IAlgebraicRewriteRule> ruleList = new LinkedList<>();
        ruleList.add(firstRule);
        ruleList.add(secondRule);
        ruleList.add(thirdRule);

        @SuppressWarnings("unchecked")
        Mutable<ILogicalOperator> root = PowerMockito.mock(Mutable.class);
        AbstractLogicalOperator rootOp = PowerMockito.mock(AbstractLogicalOperator.class);
        List<Mutable<ILogicalOperator>> emptyList = new ArrayList<>();
        PowerMockito.when(root.getValue()).thenReturn(rootOp);
        PowerMockito.when(rootOp.getInputs()).thenReturn(emptyList);

        // Case 1: the first rule returns true in the first iteration.
        //  Iteration1: R1 true, R2 false, R3 false
        //  Iteration2: R1 false, R2 false, R3 false
        PowerMockito.when(firstRule.rewritePre(any(), any())).thenReturn(true).thenReturn(false);
        PowerMockito.when(secondRule.rewritePre(any(), any())).thenReturn(false);
        PowerMockito.when(thirdRule.rewritePre(any(), any())).thenReturn(false);
        ruleController.rewriteWithRuleCollection(root, ruleList);
        // The count should be two for all rules.
        verify(firstRule, times(2)).rewritePre(any(), any());
        verify(secondRule, times(2)).rewritePre(any(), any());
        verify(thirdRule, times(2)).rewritePre(any(), any());

        // Case 2: the first rule returns false in the first iteration.
        //  Iteration1: R1 false (R2 and R3 should not be invoked.)
        reset(firstRule);
        reset(secondRule);
        reset(thirdRule);
        PowerMockito.when(firstRule.rewritePre(any(), any())).thenReturn(false);
        PowerMockito.when(secondRule.rewritePre(any(), any())).thenReturn(true);
        PowerMockito.when(thirdRule.rewritePre(any(), any())).thenReturn(true);
        ruleController.rewriteWithRuleCollection(root, ruleList);
        // The count should be one for the first rule.
        verify(firstRule, times(1)).rewritePre(any(), any());
        verify(secondRule, times(0)).rewritePre(any(), any());
        verify(thirdRule, times(0)).rewritePre(any(), any());

        // Case 3: a mixture of returning true/false.
        //  Iteration1: R1 true, R2 true, R3 false
        //  Iteration2: R1 false, R2 true, R3 false
        //  Iteration3: R1 false, R2 false, R3 false
        // So, the iteration should be stopped after the iteration 3.
        reset(firstRule);
        reset(secondRule);
        reset(thirdRule);
        PowerMockito.when(firstRule.rewritePre(any(), any())).thenReturn(true).thenReturn(false);
        PowerMockito.when(secondRule.rewritePre(any(), any())).thenReturn(true).thenReturn(true).thenReturn(false);
        PowerMockito.when(thirdRule.rewritePre(any(), any())).thenReturn(false);
        ruleController.rewriteWithRuleCollection(root, ruleList);
        // The count should be three for all rules.
        verify(firstRule, times(3)).rewritePre(any(), any());
        verify(secondRule, times(3)).rewritePre(any(), any());
        verify(thirdRule, times(3)).rewritePre(any(), any());
    }

}
