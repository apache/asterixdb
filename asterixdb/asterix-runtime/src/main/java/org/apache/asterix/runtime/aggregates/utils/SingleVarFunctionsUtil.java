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
package org.apache.asterix.runtime.aggregates.utils;

/**
The central moments of a data sample are useful for calculating statistics (such as stddev and
variance) in a single pass.

This class is based on a paper written by Philippe Pébay: Formulas for Robust, One-Pass Parallel
Computation of Covariances and Arbitrary-Order Statistical Moments, 2008, Technical Report
SAND2008-6212, Sandia National Laboratories.
*/
public class SingleVarFunctionsUtil {
    private double m1;
    private double m2;
    private long count;

    public SingleVarFunctionsUtil() {
        m1 = 0.0;
        m2 = 0.0;
        count = 0;
    }

    /**
     * Set the central moments of your current data sample
     *
     * @param  moment1  first moment (mean) of the data sample
     * @param  moment2  second moment of the data sample
     * @param  cnt      number of samples
     */
    public void set(double moment1, double moment2, long cnt) {
        m1 = moment1;
        m2 = moment2;
        count = cnt;
    }

    /**
     * Update the central moments after adding val to your data sample
     *
     * @param  val  value to add to the data sample
     */
    public void push(double val) {
        count++;
        double delta = val - m1;
        double delta_n = delta / count;
        double term1 = delta * delta_n * (count - 1);
        m1 += delta / count;
        m2 += term1;
    }

    /**
     * Combine two sets of central moments into one.
     *
     * @param  moment1  first moment (mean) of the data sample
     * @param  moment2  second moment of the data sample
     * @param  cnt      number of samples
     */
    public void combine(double moment1, double moment2, long cnt) {
        double delta = moment1 - m1;
        long combined_count = count + cnt;
        m1 = (count * m1 + cnt * moment1) / combined_count;
        m2 += moment2 + delta * delta * count * cnt / combined_count;
        count = combined_count;
    }

    public double getM1() {
        return m1;
    }

    public double getM2() {
        return m2;
    }

    public long getCount() {
        return count;
    }
}
