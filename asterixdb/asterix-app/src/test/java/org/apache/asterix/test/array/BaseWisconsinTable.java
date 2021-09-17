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
package org.apache.asterix.test.array;

import java.nio.file.Path;
import java.nio.file.Paths;

public class BaseWisconsinTable {
    public static final String TABLE_NAME = "BaseWisconsinTable";
    public static final Path TABLE_FILE = Paths.get("data", "array-index.adm");

    static final Field[] GROUPING_FIELDS =
            new Field[] { Field.GROUPING_1000, Field.GROUPING_500, Field.GROUPING_250, Field.GROUPING_100 };
    public static final int NUMBER_OF_GROUPING_FIELDS = GROUPING_FIELDS.length;
    public static final int NUMBER_OF_NON_GROUPING_FIELDS = Field.values().length - NUMBER_OF_GROUPING_FIELDS;

    @SuppressWarnings("rawtypes")
    enum Field {
        GROUPING_1000("grouping_1000", Type.BIGINT, new Domain<>(0, 999)),
        GROUPING_500("grouping_500", Type.BIGINT, new Domain<>(0, 499)),
        GROUPING_250("grouping_250", Type.BIGINT, new Domain<>(0, 249)),
        GROUPING_100("grouping_100", Type.BIGINT, new Domain<>(0, 99)),

        INTEGER_RAND_2000("integer_rand_2000", Type.BIGINT, new Domain<>(0, 1999)),
        INTEGER_SEQ_2000("integer_seq_2000", Type.BIGINT, new Domain<>(0, 1999)),
        INTEGER_SEQ_2("integer_rand_2", Type.BIGINT, new Domain<>(0, 1)),
        INTEGER_SEQ_4("integer_rand_4", Type.BIGINT, new Domain<>(0, 3)),
        INTEGER_SEQ_10("integer_rand_10", Type.BIGINT, new Domain<>(0, 9)),
        INTEGER_SEQ_20("integer_rand_20", Type.BIGINT, new Domain<>(0, 19)),

        DOUBLE_RAND_2000("double_rand_2000", Type.DOUBLE, new Domain<>(0.5, 1999.5)),
        DOUBLE_SEQ_2000("double_seq_2000", Type.DOUBLE, new Domain<>(0.5, 1999.5)),
        DOUBLE_SEQ_2("double_rand_2", Type.DOUBLE, new Domain<>(0.5, 1.5)),
        DOUBLE_SEQ_4("double_rand_4", Type.DOUBLE, new Domain<>(0.5, 3.5)),
        DOUBLE_SEQ_10("double_rand_10", Type.DOUBLE, new Domain<>(0.5, 9.5)),
        DOUBLE_SEQ_20("double_rand_20", Type.DOUBLE, new Domain<>(0.5, 19.5)),

        STRING_RAND_26_A("string_rand_26_a", Type.STRING, new Domain<>('A', 'Z')),
        STRING_RAND_26_B("string_rand_26_b", Type.STRING, new Domain<>('A', 'Z')),
        STRING_RAND_26_C("string_rand_26_c", Type.STRING, new Domain<>('A', 'Z'));

        final String fieldName;
        final Type fieldType;
        final Domain domain;

        Field(String fieldName, Type fieldType, Domain domain) {
            this.fieldName = fieldName;
            this.fieldType = fieldType;
            this.domain = domain;
        }

        enum Type {
            BIGINT,
            DOUBLE,
            STRING
        }
    }

    static class Domain<T> {
        final T minimum;
        final T maximum;

        Domain(T minimum, T maximum) {
            this.minimum = minimum;
            this.maximum = maximum;
        }
    }
}
