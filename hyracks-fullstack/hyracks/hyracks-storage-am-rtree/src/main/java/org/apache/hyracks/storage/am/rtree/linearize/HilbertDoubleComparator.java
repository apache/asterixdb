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
package org.apache.hyracks.storage.am.rtree.linearize;

import org.apache.hyracks.api.dataflow.value.ILinearizeComparator;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProvider;
import org.apache.hyracks.storage.am.rtree.impls.DoublePrimitiveValueProviderFactory;
import org.apache.hyracks.storage.common.arraylist.DoubleArrayList;
import org.apache.hyracks.storage.common.arraylist.IntArrayList;

/*
 * This compares two points based on the hilbert curve. Currently, it only supports
 * doubles (this can be changed by changing all doubles to ints as there are no
 * number generics in Java) in the two-dimensional space. For more dimensions, the
 * state machine has to be automatically generated. The idea of the fractal generation
 * of the curve is described e.g. in http://dl.acm.org/ft_gateway.cfm?id=383528&type=pdf
 *
 * Unlike the described approach, this comparator does not compute the hilbert value at
 * any point. Instead, it only evaluates how the two inputs compare to each other. This
 * is done by starting at the lowest hilbert resolution and zooming in on the fractal until
 * the two points are in different quadrants.
 *
 * As a performance optimization, the state of the state machine is saved in a stack and
 * maintained over comparisons. The idea behind this is that comparisons are usually in a
 * similar area (e.g. geo coordinates). Zooming in from [-MAX_VALUE, MAX_VALUE] would take
 * ~300 steps every time. Instead, the comparator start from the previous state and zooms out
 * if necessary
 */

public class HilbertDoubleComparator implements ILinearizeComparator {
    private final int dim; // dimension
    private final HilbertState[] states;

    private double[] bounds;
    private double stepsize;
    private int state;
    private IntArrayList stateStack = new IntArrayList(1000, 200);
    private DoubleArrayList boundsStack = new DoubleArrayList(2000, 400);

    private IPrimitiveValueProvider valueProvider =
            DoublePrimitiveValueProviderFactory.INSTANCE.createPrimitiveValueProvider();

    private double[] a;
    private double[] b;

    private class HilbertState {
        public final int[] nextState;
        public final int[] position;

        public HilbertState(int[] nextState, int[] order) {
            this.nextState = nextState;
            this.position = order;
        }
    }

    public HilbertDoubleComparator(int dimension) {
        if (dimension != 2)
            throw new IllegalArgumentException();
        dim = dimension;
        a = new double[dim];
        b = new double[dim];

        states = new HilbertState[] { new HilbertState(new int[] { 3, 0, 1, 0 }, new int[] { 0, 1, 3, 2 }),
                new HilbertState(new int[] { 1, 1, 0, 2 }, new int[] { 2, 1, 3, 0 }),
                new HilbertState(new int[] { 2, 3, 2, 1 }, new int[] { 2, 3, 1, 0 }),
                new HilbertState(new int[] { 0, 2, 3, 3 }, new int[] { 0, 3, 1, 2 }) };

        resetStateMachine();
    }

    private void resetStateMachine() {
        state = 0;
        stateStack.clear();
        stepsize = Double.MAX_VALUE / 2;
        bounds = new double[dim];
        boundsStack.clear();
    }

    public int compare() {
        boolean equal = true;
        for (int i = 0; i < dim; i++) {
            if (a[i] != b[i])
                equal = false;
        }
        if (equal)
            return 0;

        // We keep the state of the state machine after a comparison. In most
        // cases,
        // the needed zoom factor is close to the old one. In this step, we
        // check if we have
        // to zoom out
        while (true) {
            if (stateStack.size() <= dim) {
                resetStateMachine();
                break;
            }
            boolean zoomOut = false;
            for (int i = 0; i < dim; i++) {
                if (Math.min(a[i], b[i]) <= bounds[i] - 2 * stepsize
                        || Math.max(a[i], b[i]) >= bounds[i] + 2 * stepsize) {
                    zoomOut = true;
                    break;
                }
            }
            state = stateStack.getLast();
            stateStack.removeLast();
            for (int j = dim - 1; j >= 0; j--) {
                bounds[j] = boundsStack.getLast();
                boundsStack.removeLast();
            }
            stepsize *= 2;
            if (!zoomOut) {
                state = stateStack.getLast();
                stateStack.removeLast();
                for (int j = dim - 1; j >= 0; j--) {
                    bounds[j] = boundsStack.getLast();
                    boundsStack.removeLast();
                }
                stepsize *= 2;
                break;
            }
        }

        while (true) {
            stateStack.add(state);
            for (int j = 0; j < dim; j++) {
                boundsStack.add(bounds[j]);
            }

            // Find the quadrant in which A and B are
            int quadrantA = 0, quadrantB = 0;
            for (int i = dim - 1; i >= 0; i--) {
                if (a[i] >= bounds[i])
                    quadrantA ^= (1 << (dim - i - 1));
                if (b[i] >= bounds[i])
                    quadrantB ^= (1 << (dim - i - 1));

                if (a[i] >= bounds[i]) {
                    bounds[i] += stepsize;
                } else {
                    bounds[i] -= stepsize;
                }
            }

            stepsize /= 2;
            if (stepsize <= 2 * DoublePointable.getEpsilon())
                return 0;
            // avoid infinite loop due to machine epsilon problems

            if (quadrantA != quadrantB) {
                // find the position of A and B's quadrants
                int posA = states[state].position[quadrantA];
                int posB = states[state].position[quadrantB];

                if (posA < posB)
                    return -1;
                else
                    return 1;
            }

            state = states[state].nextState[quadrantA];
        }
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        for (int i = 0; i < dim; i++) {
            a[i] = DoublePointable.getDouble(b1, s1 + (i * l1));
            b[i] = DoublePointable.getDouble(b2, s2 + (i * l2));
        }

        return compare();
    }

    @Override
    public int getDimensions() {
        return dim;
    }
}
