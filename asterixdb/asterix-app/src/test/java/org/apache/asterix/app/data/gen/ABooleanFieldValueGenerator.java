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
import java.util.Random;

import org.apache.asterix.app.data.gen.RecordTupleGenerator.GenerationFunction;
import org.apache.asterix.om.types.ATypeTag;

public class ABooleanFieldValueGenerator implements IAsterixFieldValueGenerator<Boolean> {
    private final GenerationFunction generationFunction;
    private final boolean tagged;
    private final Random rand = new Random();
    private boolean value;

    public ABooleanFieldValueGenerator(GenerationFunction generationFunction, boolean tagged) {
        this.generationFunction = generationFunction;
        this.tagged = tagged;
        switch (generationFunction) {
            case DECREASING:
                value = true;
                break;
            case DETERMINISTIC:
                value = false;
                break;
            case INCREASING:
                value = false;
                break;
            case RANDOM:
                value = rand.nextBoolean();
            default:
                break;
        }
    }

    @Override
    public void next(DataOutput out) throws IOException {
        if (tagged) {
            out.writeByte(ATypeTag.SERIALIZED_BOOLEAN_TYPE_TAG);
        }
        generate();
        out.writeBoolean(value);
    }

    private void generate() {
        switch (generationFunction) {
            case DETERMINISTIC:
                value = !value;
                break;
            case RANDOM:
                value = rand.nextBoolean();
                break;
            default:
                break;
        }
    }

    @Override
    public Boolean next() throws IOException {
        generate();
        return value;
    }

    @Override
    public void get(DataOutput out) throws IOException {
        if (tagged) {
            out.writeByte(ATypeTag.SERIALIZED_BOOLEAN_TYPE_TAG);
        }
        out.writeBoolean(value);
    }

    @Override
    public Boolean get() throws IOException {
        return value;
    }
}
