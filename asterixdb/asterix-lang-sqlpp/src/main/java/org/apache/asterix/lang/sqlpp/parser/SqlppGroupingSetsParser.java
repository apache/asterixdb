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

package org.apache.asterix.lang.sqlpp.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Parses GROUP BY's grouping elements into GROUPING SETS:
 * <p>
 * GROUP BY {0},{1},... => GROUP BY GROUPING SETS (...)
 */
public final class SqlppGroupingSetsParser {

    // max number of grouping sets in a single group by clause
    static final int GROUPING_SETS_LIMIT = 128;

    private SourceLocation sourceLoc;

    private List<GroupingElement> workList;

    private List<GbyVariableExpressionPair> gbyPairWorkList;

    private LinkedHashMap<Expression, GbyVariableExpressionPair> gbyPairWorkMap;

    private List<List<List<GbyVariableExpressionPair>>> crossProductWorkLists;

    public List<List<GbyVariableExpressionPair>> parse(List<GroupingElement> inList, SourceLocation sourceLoc)
            throws CompilationException {
        this.sourceLoc = sourceLoc;
        List<GbyVariableExpressionPair> primitiveList = transformSimpleToPrimitive(inList);
        if (primitiveList != null) {
            return Collections.singletonList(primitiveList);
        } else {
            if (workList == null) {
                workList = new ArrayList<>();
            } else {
                workList.clear();
            }
            eliminateComplexGroupingSets(inList, workList, false);
            return crossProductGroupingSets(workList);
        }
    }

    private List<GbyVariableExpressionPair> transformSimpleToPrimitive(List<GroupingElement> inList)
            throws CompilationException {
        if (findComplexElement(inList) != null) {
            return null;
        }
        if (gbyPairWorkList == null) {
            gbyPairWorkList = new ArrayList<>();
        } else {
            gbyPairWorkList.clear();
        }
        concatOrdinary(inList, inList.size(), gbyPairWorkList);
        return removeDuplicates(gbyPairWorkList);
    }

    private void eliminateComplexGroupingSets(List<? extends GroupingElement> inList, List<GroupingElement> outList,
            boolean isInsideGroupingSets) throws CompilationException {
        for (GroupingElement element : inList) {
            switch (element.getKind()) {
                case GROUPING_SET:
                    outList.add(element);
                    break;
                case ROLLUP_CUBE:
                    RollupCube rollupCube = (RollupCube) element;
                    List<GroupingSet> rollupCubeTransform = expandRollupCube(rollupCube);
                    if (isInsideGroupingSets) {
                        outList.addAll(rollupCubeTransform);
                    } else { // top level
                        outList.add(new GroupingSets(rollupCubeTransform));
                    }
                    break;
                case GROUPING_SETS:
                    GroupingSets groupingSets = (GroupingSets) element;
                    List<? extends GroupingElement> groupingSetsItems = groupingSets.getItems();
                    List<GroupingElement> groupingSetsTransform = new ArrayList<>(groupingSetsItems.size());
                    eliminateComplexGroupingSets(groupingSetsItems, groupingSetsTransform, true);
                    if (isInsideGroupingSets) {
                        outList.addAll(groupingSetsTransform);
                    } else { // top level
                        outList.add(new GroupingSets(groupingSetsTransform));
                    }
                    break;
                default:
                    throw new IllegalStateException(String.valueOf(element.getKind()));
            }
        }
    }

    private List<GroupingSet> expandRollupCube(RollupCube rollupCube) throws CompilationException {
        List<GroupingSet> rollupCubeItems = rollupCube.getItems();
        return rollupCube.isCube() ? expandCube(rollupCubeItems) : expandRollup(rollupCubeItems);
    }

    private List<GroupingSet> expandRollup(List<GroupingSet> items) throws CompilationException {
        int n = items.size();
        int rollupSize = n + 1;
        checkGroupingSetsLimit(rollupSize);
        List<GroupingSet> result = new ArrayList<>(rollupSize);
        for (int i = n; i > 0; i--) {
            List<GbyVariableExpressionPair> groupingSetItems = new ArrayList<>();
            concatOrdinary(items, i, groupingSetItems);
            result.add(new GroupingSet(groupingSetItems));
        }
        result.add(GroupingSet.EMPTY);
        return result;
    }

    private List<GroupingSet> expandCube(List<GroupingSet> items) throws CompilationException {
        int n = items.size();
        int cubeSize = 1 << n;
        checkGroupingSetsLimit(cubeSize);
        List<GroupingSet> result = new ArrayList<>(cubeSize);
        List<GroupingSet> permutation = new ArrayList<>(n);
        for (long v = cubeSize - 1; v > 0; v--) {
            permutation.clear();
            for (int i = n - 1; i >= 0; i--) {
                if ((v & (1L << i)) != 0) {
                    permutation.add(items.get(n - i - 1));
                }
            }
            List<GbyVariableExpressionPair> groupingSetItems = new ArrayList<>();
            concatOrdinary(permutation, permutation.size(), groupingSetItems);
            result.add(new GroupingSet(groupingSetItems));
        }
        result.add(GroupingSet.EMPTY);
        return result;
    }

    private List<List<GbyVariableExpressionPair>> crossProductGroupingSets(List<GroupingElement> inList)
            throws CompilationException {
        if (crossProductWorkLists == null) {
            crossProductWorkLists = Arrays.asList(new ArrayList<>(), new ArrayList<>());
        } else {
            for (List<List<GbyVariableExpressionPair>> list : crossProductWorkLists) {
                list.clear();
            }
        }

        int workInListIdx = 0;
        for (int inListPos = 0, inListSize = inList.size(); inListPos < inListSize; inListPos++) {
            List<List<GbyVariableExpressionPair>> workInList = crossProductWorkLists.get(workInListIdx);
            int workOutListIdx = 1 - workInListIdx;
            List<List<GbyVariableExpressionPair>> workOutList = crossProductWorkLists.get(workOutListIdx);
            workOutList.clear();

            GroupingElement element = inList.get(inListPos);
            GroupingSet groupingSet = null;
            GroupingSets groupingSets = null;
            switch (element.getKind()) {
                case GROUPING_SET:
                    groupingSet = (GroupingSet) element;
                    break;
                case GROUPING_SETS:
                    groupingSets = (GroupingSets) element;
                    break;
                default:
                    throw new IllegalStateException(String.valueOf(element.getKind()));
            }

            if (inListPos == 0) {
                if (groupingSet != null) {
                    workOutList.add(groupingSet.getItems());
                } else {
                    for (GroupingElement item : groupingSets.getItems()) {
                        workOutList.add(((GroupingSet) item).getItems());
                    }
                }
            } else {
                for (List<GbyVariableExpressionPair> workGroupingSet : workInList) {
                    if (groupingSet != null) {
                        workOutList.add(concatOrdinary(workGroupingSet, groupingSet.getItems()));
                    } else {
                        for (GroupingElement groupingElement : groupingSets.getItems()) {
                            workOutList
                                    .add(concatOrdinary(workGroupingSet, ((GroupingSet) groupingElement).getItems()));
                        }
                    }
                }
            }

            checkGroupingSetsLimit(workOutList.size());

            workInListIdx = workOutListIdx;
        }

        List<List<GbyVariableExpressionPair>> crossProductList = crossProductWorkLists.get(workInListIdx);

        // check for unexpected aliases
        Map<Expression, GbyVariableExpressionPair> gbyPairMap = new HashMap<>();
        List<List<GbyVariableExpressionPair>> result = new ArrayList<>(crossProductList.size());
        for (List<GbyVariableExpressionPair> groupingSet : crossProductList) {
            List<GbyVariableExpressionPair> gsNoDups = removeDuplicates(groupingSet);
            for (int i = 0, n = gsNoDups.size(); i < n; i++) {
                GbyVariableExpressionPair gbyPair = gsNoDups.get(i);
                GbyVariableExpressionPair existingPair = gbyPairMap.get(gbyPair.getExpr());
                if (existingPair == null) {
                    gbyPairMap.put(gbyPair.getExpr(), gbyPair);
                } else if (!Objects.equals(existingPair.getVar(), gbyPair.getVar())) {
                    if (gbyPair.getVar() != null) {
                        // existing pair's alias was different or null
                        throw new CompilationException(ErrorCode.COMPILATION_UNEXPECTED_ALIAS,
                                gbyPair.getVar().getSourceLocation(),
                                SqlppVariableUtil.toUserDefinedName(gbyPair.getVar().getVar().getValue()));
                    } else {
                        // this pair's alias is null, but the existing one was not null -> use the existing one
                        VariableExpr newVar = new VariableExpr(new VarIdentifier(existingPair.getVar().getVar()));
                        newVar.setSourceLocation(existingPair.getVar().getSourceLocation());
                        gbyPair = new GbyVariableExpressionPair(newVar, gbyPair.getExpr());
                        gsNoDups.set(i, gbyPair);
                    }
                }
            }
            result.add(gsNoDups);
        }
        return result;
    }

    private List<GbyVariableExpressionPair> removeDuplicates(List<GbyVariableExpressionPair> inList)
            throws CompilationException {
        if (gbyPairWorkMap == null) {
            gbyPairWorkMap = new LinkedHashMap<>();
        } else {
            gbyPairWorkMap.clear();
        }
        for (GbyVariableExpressionPair gbyPair : inList) {
            GbyVariableExpressionPair existingPair = gbyPairWorkMap.get(gbyPair.getExpr());
            if (existingPair == null) {
                gbyPairWorkMap.put(gbyPair.getExpr(), gbyPair);
            } else if (!Objects.equals(existingPair.getVar(), gbyPair.getVar())) {
                if (gbyPair.getVar() != null) {
                    // existing pair's alias was different or null
                    throw new CompilationException(ErrorCode.COMPILATION_UNEXPECTED_ALIAS,
                            gbyPair.getVar().getSourceLocation(),
                            SqlppVariableUtil.toUserDefinedName(gbyPair.getVar().getVar().getValue()));
                }
                // otherwise this pair's alias is null, but the existing one was not null -> use the existing one
            }
        }
        return new ArrayList<>(gbyPairWorkMap.values());
    }

    private List<GbyVariableExpressionPair> concatOrdinary(List<GbyVariableExpressionPair> groupingSet1,
            List<GbyVariableExpressionPair> groupingSet2) {
        List<GbyVariableExpressionPair> outList = new ArrayList<>(groupingSet1.size() + groupingSet2.size());
        outList.addAll(groupingSet1);
        outList.addAll(groupingSet2);
        return outList;
    }

    private void concatOrdinary(List<? extends GroupingElement> inList, int endIdx,
            List<GbyVariableExpressionPair> outList) {
        for (int i = 0; i < endIdx; i++) {
            GroupingElement element = inList.get(i);
            if (element.getKind() != GroupingElement.Kind.GROUPING_SET) {
                throw new IllegalStateException(String.valueOf(element.getKind()));
            }
            outList.addAll(((GroupingSet) element).getItems());
        }
    }

    private static GroupingElement findComplexElement(List<GroupingElement> inList) {
        for (GroupingElement element : inList) {
            switch (element.getKind()) {
                case ROLLUP_CUBE:
                case GROUPING_SETS:
                    return element;
                case GROUPING_SET:
                    break;
                default:
                    throw new IllegalStateException(String.valueOf(element.getKind()));
            }
        }
        return null;
    }

    private void checkGroupingSetsLimit(int n) throws CompilationException {
        if (n > GROUPING_SETS_LIMIT) {
            throw new CompilationException(ErrorCode.COMPILATION_GROUPING_SETS_OVERFLOW, sourceLoc, String.valueOf(n),
                    String.valueOf(GROUPING_SETS_LIMIT));
        }
    }

    public abstract static class GroupingElement {
        public enum Kind {
            GROUPING_SET,
            GROUPING_SETS,
            ROLLUP_CUBE,
        }

        public abstract Kind getKind();
    }

    // ordinary grouping set, empty grouping set
    public static final class GroupingSet extends GroupingElement {

        public static final GroupingSet EMPTY = new GroupingSet(Collections.emptyList());

        private final List<GbyVariableExpressionPair> items;

        public GroupingSet(List<GbyVariableExpressionPair> items) {
            this.items = items;
        }

        @Override
        public Kind getKind() {
            return Kind.GROUPING_SET;
        }

        public List<GbyVariableExpressionPair> getItems() {
            return items;
        }
    }

    public static final class RollupCube extends GroupingElement {

        private final List<GroupingSet> items;

        private final boolean isCube;

        public RollupCube(List<GroupingSet> items, boolean isCube) {
            this.items = items;
            this.isCube = isCube;
        }

        @Override
        public Kind getKind() {
            return Kind.ROLLUP_CUBE;
        }

        public List<GroupingSet> getItems() {
            return items;
        }

        public boolean isCube() {
            return isCube;
        }
    }

    public static final class GroupingSets extends GroupingElement {

        private final List<? extends GroupingElement> items;

        public GroupingSets(List<? extends GroupingElement> items) {
            this.items = items;
        }

        @Override
        public Kind getKind() {
            return Kind.GROUPING_SETS;
        }

        public List<? extends GroupingElement> getItems() {
            return items;
        }
    }
}
