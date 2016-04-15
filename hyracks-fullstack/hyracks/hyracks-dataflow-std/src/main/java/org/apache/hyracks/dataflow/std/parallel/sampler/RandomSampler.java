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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class RandomSampler<T> implements ISampler<T>, Iterator<T> {

    private final List<T> elements = new ArrayList<T>();

    private int fixCard;

    private Random rand;

    private int curCard;

    public RandomSampler(final Collection<T> base) {
        Iterator<T> iter = base.iterator();
        while (iter.hasNext())
            elements.add(iter.next());
    }

    public RandomSampler(final Collection<T> base, int fixCard) {
        this(base);
        this.fixCard = fixCard;
        curCard = fixCard;
        rand = new Random();
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
        Collection<T> ret = new HashSet<T>();
        while (hasNext())
            ret.add(next());
        return ret;
    }

    @Override
    public int getSize() {
        return curCard;
    }

    @Override
    public boolean sampleNext() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void remove() {
        curCard--;
    }

    @Override
    public boolean hasNext() {
        return (curCard > 0);
    }

    @Override
    public T next() {
        curCard--;
        return elements.remove(Math.abs(rand.nextInt()) % elements.size());
    }

    @Override
    public void reset() {
        curCard = fixCard;
    }
}
