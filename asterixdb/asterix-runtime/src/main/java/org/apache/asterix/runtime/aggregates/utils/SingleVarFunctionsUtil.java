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
    private double m3;
    private double m4;
    private long count;
    private boolean m3Flag;
    private boolean m4Flag;

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
     * @param  moment3  third moment of the data sample
     * @param  moment4  fourth moment of the data sample
     * @param  cnt      number of samples
     * @param  moment3Flag   boolean flag to update the value of the third moment when adding values to the data sample
     * @param  moment4Flag   boolean flag to update the value of the fourth moment when adding values to the data sample
     */
    public void set(double moment1, double moment2, double moment3, double moment4, long cnt, boolean moment3Flag,
            boolean moment4Flag) {
        m1 = moment1;
        m2 = moment2;
        m3 = moment3;
        m4 = moment4;
        count = cnt;
        m3Flag = moment3Flag;
        m4Flag = moment4Flag;
    }

    /**
     * Update the central moments after adding val to your data sample
     *
     * @param  val      value to add to the data sample
     */
    public void push(double val) {
        count++;
        double delta = val - m1;
        double delta_n = delta / count;
        double term1 = delta * delta_n * (count - 1);
        m1 += delta / count;
        if (m4Flag) {
            m4 += term1 * delta_n * delta_n * (count * count - 3 * count + 3);
            m4 += 6 * delta_n * delta_n * m2 - 4 * delta_n * m3;
        }
        if (m3Flag) {
            m3 += term1 * delta_n * (count - 2) - 3 * delta_n * m2;
        }
        m2 += term1;
    }

    /**
     * Combine two sets of central moments into one.
     *
     * @param  moment1  first moment (mean) of the data sample
     * @param  moment2  second moment of the data sample
     * @param  moment3  third moment of the data sample
     * @param  moment4  fourth moment of the data sample
     * @param  cnt      number of samples
     */
    public void combine(double moment1, double moment2, double moment3, double moment4, long cnt) {
        double delta = moment1 - m1;
        long combined_count = count + cnt;
        if (m3Flag) {
            double delta3 = delta * delta * delta;
            if (m4Flag) {
                m4 += moment4 + delta3 * delta * count * cnt * (count * count - count * cnt + cnt * cnt)
                        / (combined_count * combined_count * combined_count);
                m4 += 6 * delta * delta * (count * count * moment2 + cnt * cnt * m2) / (combined_count * combined_count)
                        + 4 * delta * (count * moment3 - cnt * m3) / combined_count;
            }
            m3 += moment3 + delta3 * count * cnt * (count - cnt) / (combined_count * combined_count);
            m3 += 3 * delta * (count * moment2 - cnt * m2) / combined_count;
        }
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

    public double getM3() {
        return m3;
    }

    public double getM4() {
        return m4;
    }

    public long getCount() {
        return count;
    }
}
