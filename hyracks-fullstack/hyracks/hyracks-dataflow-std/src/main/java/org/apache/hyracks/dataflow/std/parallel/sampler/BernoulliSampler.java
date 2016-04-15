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

package org.apache.hyracks.dataflow.std.parallel.sampler;

import java.util.Collection;
import java.util.Random;

public class BernoulliSampler<T> implements ISampler<T> {
    private final double percent;

    private final Random rnd;

    private Double nextRnd = null;

    public BernoulliSampler(double percent) {
        this.percent = percent / 100;
        rnd = new Random();
    }

    public BernoulliSampler(double percent, Random rnd) {
        this.percent = percent;
        this.rnd = rnd;
        stage();
    }

    private void stage() {
        nextRnd = rnd.nextDouble();
    }

    private boolean check() {
        return nextRnd < percent;
    }

    public void setSeed(long seed) {
        rnd.setSeed(seed);
    }

    @Override
    public void sample(T t) {
        // TODO Auto-generated method stub
    }

    @SuppressWarnings("unchecked")
    @Override
    public void sample(T... t) {
        // TODO Auto-generated method stub
    }

    @Override
    public Collection<T> getSamples() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getSize() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void reset() {
        // TODO Auto-generated method stub
    }

    @Override
    public boolean sampleNext() {
        boolean val = check();
        stage();
        return val;
    }

    @Override
    public void remove() {
        // TODO Auto-generated method stub
    }
}
