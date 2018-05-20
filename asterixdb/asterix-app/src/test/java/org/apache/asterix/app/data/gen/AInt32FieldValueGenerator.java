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
package org.apache.asterix.app.data.gen;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.asterix.app.data.gen.RecordTupleGenerator.GenerationFunction;
import org.apache.asterix.om.types.ATypeTag;

public class AInt32FieldValueGenerator implements IAsterixFieldValueGenerator<Integer> {
    private static final int START = 1000000000;
    private static final int BATCH_SIZE = 1000;
    private final GenerationFunction generationFunction;
    private final boolean unique;
    private final boolean tagged;
    private final Random rand = new Random();
    private int value;
    private int cycle;
    private List<Integer> uniques;
    private Iterator<Integer> iterator;

    public AInt32FieldValueGenerator(GenerationFunction generationFunction, boolean unique, boolean tagged) {
        this.generationFunction = generationFunction;
        this.unique = unique;
        this.tagged = tagged;
        switch (generationFunction) {
            case DECREASING:
                value = Integer.MAX_VALUE;
                break;
            case DETERMINISTIC:
                value = START;
                break;
            case INCREASING:
                value = 0;
                break;
            case RANDOM:
                if (unique) {
                    int lowerBound = START;
                    int upperBound = lowerBound + BATCH_SIZE;
                    uniques = new ArrayList<>();
                    while (lowerBound < upperBound) {
                        uniques.add(lowerBound);
                        lowerBound++;
                    }
                    Collections.shuffle(uniques);
                    iterator = uniques.iterator();
                }
            default:
                break;
        }
    }

    @Override
    public void next(DataOutput out) throws IOException {
        if (tagged) {
            out.writeByte(ATypeTag.SERIALIZED_INT32_TYPE_TAG);
        }
        generate();
        out.writeInt(value);
    }

    private void generate() {
        switch (generationFunction) {
            case DECREASING:
                value--;
            case DETERMINISTIC:
                if (value >= START) {
                    cycle++;
                    value = START - cycle;
                } else {
                    value = START + cycle;
                }
                break;
            case INCREASING:
                value++;
                break;
            case RANDOM:
                if (unique) {
                    if (iterator.hasNext()) {
                        value = iterator.next();
                    } else {
                        // generate next patch
                        cycle++;
                        int lowerBound;
                        if (cycle % 2 == 0) {
                            // even
                            lowerBound = START + ((cycle / 2) * BATCH_SIZE);
                        } else {
                            // odd
                            lowerBound = START - ((cycle / 2 + 1) * BATCH_SIZE);
                        }
                        int upperBound = lowerBound + BATCH_SIZE;
                        uniques.clear();
                        while (lowerBound < upperBound) {
                            uniques.add(lowerBound);
                            lowerBound++;
                        }
                        Collections.shuffle(uniques);
                        iterator = uniques.iterator();
                        value = iterator.next();
                    }
                } else {
                    value = rand.nextInt();
                }
                break;
            default:
                break;

        }
    }

    @Override
    public Integer next() throws IOException {
        generate();
        return value;
    }

    @Override
    public void get(DataOutput out) throws IOException {
        if (tagged) {
            out.writeByte(ATypeTag.SERIALIZED_INT32_TYPE_TAG);
        }
        out.writeInt(value);
    }

    @Override
    public Integer get() throws IOException {
        return value;
    }
}
