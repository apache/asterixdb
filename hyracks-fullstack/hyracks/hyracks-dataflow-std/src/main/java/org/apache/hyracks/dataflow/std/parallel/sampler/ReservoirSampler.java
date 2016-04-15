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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class ReservoirSampler<T extends Comparable<T>> implements ISampler<T> {

    public interface IGammaFunction {
        enum GAMMA_TYPE {
            RANDOM,
            ZIPFAN
        }

        long apply(long t);
    }

    public class SimpleRandom implements IGammaFunction {

        private final Random RAND = new Random();

        private long n;

        public SimpleRandom(int n) {
            super();
            this.n = n;
        }

        @Override
        public long apply(long t) {
            int skipCount = 0;
            while (RAND.nextDouble() * t >= n)
                skipCount++;
            return skipCount;
        }
    }

    public class SimpleZipfan implements IGammaFunction {

        private Random generator = new Random();

        private final int n;

        private double w;

        public SimpleZipfan(int n) {
            super();
            this.n = n;
            this.w = Math.exp(-Math.log(generator.nextDouble()) / n);
        }

        @Override
        public long apply(long t) {
            double term = t - this.n + 1;
            double u;
            double x;
            long gamma;
            while (true) {
                //generate u and x
                u = generator.nextDouble();
                x = t * (this.w - 1.0);
                gamma = (long) x;
                //test if u <= h(gamma)/cg(x)
                double lhs = Math.exp(Math.log(((u * Math.pow(((t + 1) / term), 2)) * (term + gamma)) / (t + x))
                        / this.n);
                double rhs = (((t + x) / (term + gamma)) * term) / t;
                if (lhs < rhs) {
                    this.w = rhs / lhs;
                    break;
                }
                //test if u <= f(gamma)/cg(x)
                double y = (((u * (t + 1)) / term) * (t + gamma + 1)) / (t + x);
                double denom;
                double number_lim;
                if (this.n < gamma) {
                    denom = t;
                    number_lim = term + gamma;
                } else {
                    denom = t - this.n + gamma;
                    number_lim = t + 1;
                }

                for (long number = t + gamma; number >= number_lim; number--) {
                    y = (y * number) / denom;
                    denom = denom - 1;
                }
                this.w = Math.exp(-Math.log(generator.nextDouble()) / this.n);
                if (Math.exp(Math.log(y) / this.n) <= (t + x) / t) {
                    break;
                }
            }
            return gamma;
        }
    }

    List<T> elements;

    private int size;

    private boolean orderedExport = false;

    private long skipCount;

    private int currentCount;

    private IGammaFunction skipFunction;

    private IGammaFunction.GAMMA_TYPE type = IGammaFunction.GAMMA_TYPE.ZIPFAN;

    private final Random RANDOM = new Random();

    public ReservoirSampler(int size) {
        elements = new ArrayList<T>(size);
        this.size = size;
        this.currentCount = 0;
        this.skipCount = 0;
        if (type.equals(IGammaFunction.GAMMA_TYPE.ZIPFAN))
            this.skipFunction = new SimpleZipfan(size);
        else
            this.skipFunction = new SimpleRandom(size);
    }

    public ReservoirSampler(int size, boolean orderedExport) {
        this(size);
        this.orderedExport = orderedExport;
    }

    @Override
    public void sample(T t) {
        if (size != elements.size()) {
            elements.add(t);
        } else {
            if (skipCount > 0) {
                skipCount--;
            } else {
                elements.set(RANDOM.nextInt(size), t);
                skipCount = skipFunction.apply(currentCount);
            }
        }

        currentCount++;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void sample(T... t) {
        for (T item : t)
            sample(item);
    }

    @Override
    public Collection<T> getSamples() {
        if (orderedExport)
            Collections.sort(elements);
        return Collections.unmodifiableCollection(elements);
    }

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public void reset() {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean sampleNext() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void remove() {
        // TODO Auto-generated method stub

    }

}
