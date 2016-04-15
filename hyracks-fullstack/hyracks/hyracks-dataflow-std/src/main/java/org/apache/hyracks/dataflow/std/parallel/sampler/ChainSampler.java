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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class ChainSampler<T> implements ISampler<T> {
    private int k;

    private int n;

    private long count = 0;

    private int fillSample = 0;

    private List<T> elements;

    private Map<Integer, Integer> replacements;

    private Random random = new Random();

    public ChainSampler(int k, int n) {
        this.k = k;
        this.n = n;
        elements = new ArrayList<T>(k);
        replacements = new HashMap<Integer, Integer>();
    }

    @Override
    public void sample(T t) {
        int i = (int) (count % n);
        if (replacements.containsKey(i)) {
            int replace = replacements.get(i);
            elements.set(replace, t);
            int next = random.nextInt(n);
            replacements.remove(i);
            replacements.put(next, replace);
        } else if (fillSample < k) {
            double prob = ((double)Math.min(i, n)) / ((double)n);
            if (random.nextDouble() < prob) {
                int bucket = fillSample++;
                int next = random.nextInt(n);
                elements.set(bucket, t);
                replacements.put(next, bucket);
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void sample(T... t) {
        for (T item : t)
            sample(item);
    }

    @Override
    public Collection<T> getSamples() {
        return Collections.unmodifiableCollection(elements);
    }

    @Override
    public int getSize() {
        return (fillSample < k) ? fillSample : k;
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
