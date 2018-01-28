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

package org.apache.hyracks.storage.am.rtree.impls;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProvider;

public class Rectangle {
    private int dim;
    private double[] low;
    private double[] high;

    public Rectangle(int dim) {
        this.dim = dim;
        low = new double[this.dim];
        high = new double[this.dim];
    }

    public int getDim() {
        return dim;
    }

    public double getLow(int i) {
        return low[i];
    }

    public double getHigh(int i) {
        return high[i];
    }

    public void setLow(int i, double value) {
        low[i] = value;
    }

    public void setHigh(int i, double value) {
        high[i] = value;
    }

    public void set(ITupleReference tuple, IPrimitiveValueProvider[] valueProviders) {
        for (int i = 0; i < getDim(); i++) {
            int j = i + getDim();
            setLow(i, valueProviders[i].getValue(tuple.getFieldData(i), tuple.getFieldStart(i)));
            setHigh(i, valueProviders[j].getValue(tuple.getFieldData(j), tuple.getFieldStart(j)));
        }
    }

    public void enlarge(ITupleReference tupleToBeInserted, IPrimitiveValueProvider[] valueProviders) {
        for (int i = 0; i < getDim(); i++) {
            int j = getDim() + i;
            double low =
                    valueProviders[i].getValue(tupleToBeInserted.getFieldData(i), tupleToBeInserted.getFieldStart(i));
            if (getLow(i) > low) {
                setLow(i, low);
            }
            double high =
                    valueProviders[j].getValue(tupleToBeInserted.getFieldData(j), tupleToBeInserted.getFieldStart(j));
            if (getHigh(i) < high) {
                setHigh(i, high);
            }
        }
    }

    public double enlargedArea(ITupleReference tupleToBeInserted, IPrimitiveValueProvider[] valueProviders) {
        double areaBeforeEnlarge = area();
        double areaAfterEnlarge = 1.0;

        for (int i = 0; i < getDim(); i++) {
            int j = getDim() + i;

            double low =
                    valueProviders[i].getValue(tupleToBeInserted.getFieldData(i), tupleToBeInserted.getFieldStart(i));
            double lowAfterEnlargement;
            if (getLow(i) > low) {
                lowAfterEnlargement = low;
            } else {
                lowAfterEnlargement = getLow(i);
            }

            double high =
                    valueProviders[j].getValue(tupleToBeInserted.getFieldData(j), tupleToBeInserted.getFieldStart(j));
            double highAfterEnlargement;
            if (getHigh(i) < high) {
                highAfterEnlargement = high;
            } else {
                highAfterEnlargement = getHigh(i);
            }

            areaAfterEnlarge *= highAfterEnlargement - lowAfterEnlargement;
        }
        return areaAfterEnlarge - areaBeforeEnlarge;
    }

    public double margin() {
        double margin = 0.0;
        double mul = Math.pow(2, (double) getDim() - 1.0);
        for (int i = 0; i < getDim(); i++) {
            margin += (getHigh(i) - getLow(i)) * mul;
        }
        return margin;
    }

    public double overlappedArea(Rectangle rec) {
        double area = 1.0;
        double f1, f2;

        for (int i = 0; i < getDim(); i++) {
            if (getLow(i) > rec.getHigh(i) || getHigh(i) < rec.getLow(i)) {
                return 0.0;
            }

            f1 = Math.max(getLow(i), rec.getLow(i));
            f2 = Math.min(getHigh(i), rec.getHigh(i));
            area *= f2 - f1;
        }
        return area;
    }

    public double area() {
        double area = 1.0;
        for (int i = 0; i < getDim(); i++) {
            area *= getHigh(i) - getLow(i);
        }
        return area;
    }
}
