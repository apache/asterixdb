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

/*
 * This compares two points based on the z curve. For doubles, we cannot use
 * the simple bit magic approach. There may, however, be a better approach than this.
 */

public class ZCurveDoubleComparator implements ILinearizeComparator {
    private final int dim; // dimension

    private double[] bounds;
    private double stepsize;
    private DoubleArrayList boundsStack = new DoubleArrayList(2000, 400);

    private IPrimitiveValueProvider valueProvider =
            DoublePrimitiveValueProviderFactory.INSTANCE.createPrimitiveValueProvider();

    private double[] a;
    private double[] b;

    public ZCurveDoubleComparator(int dimension) {
        dim = dimension;
        a = new double[dim];
        b = new double[dim];

        resetStateMachine();
    }

    private void resetStateMachine() {
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
            if (boundsStack.size() <= dim) {
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

            for (int j = dim - 1; j >= 0; j--) {
                bounds[j] = boundsStack.getLast();
                boundsStack.removeLast();
            }
            stepsize *= 2;
            if (!zoomOut) {
                for (int j = dim - 1; j >= 0; j--) {
                    bounds[j] = boundsStack.getLast();
                    boundsStack.removeLast();
                }
                stepsize *= 2;
                break;
            }
        }

        while (true) {
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
                if (quadrantA < quadrantB)
                    return -1;
                else if (quadrantA > quadrantB)
                    return 1;
                else
                    return 0;
            }
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
