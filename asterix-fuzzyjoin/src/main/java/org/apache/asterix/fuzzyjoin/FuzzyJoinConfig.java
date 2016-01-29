/**
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

package org.apache.asterix.fuzzyjoin;

public class FuzzyJoinConfig {
    private static final String NAMESPACE = "fuzzyjoin";
    //
    // tokenizer
    //
    public static final String TOKENIZER_PROPERTY = NAMESPACE + ".tokenizer";
    public static final String TOKENIZER_VALUE = "Word";
    //
    // similarity
    //
    public static final String SIMILARITY_NAME_PROPERTY = NAMESPACE + ".similarity.name";
    public static final String SIMILARITY_NAME_VALUE = "Jaccard";
    public static final String SIMILARITY_THRESHOLD_PROPERTY = NAMESPACE + ".similarity.threshold";
    public static final float SIMILARITY_THRESHOLD_VALUE = .8f;
    //
    // record
    //
    public static final String RECORD_DATA_PROPERTY = NAMESPACE + ".record.data";
    public static final String RECORD_DATA_VALUE = "1";
    public static final String RECORD_DATA1_PROPERTY = NAMESPACE + ".record.data1";
    public static final String RECORD_DATA1_VALUE = "1";
    //
    // data
    //
    public static final String DATA_TOKENS_PROPERTY = NAMESPACE + ".data.tokens";
    //
    // const
    //
    public static final String RECORD_DATA_VALUE_SEPARATOR_REGEX = ",";
    public static final char WORD_SEPARATOR = '_';
    public static final String WORD_SEPARATOR_REGEX = "_";
    public static final char TOKEN_SEPARATOR = '_';
    public static final String TOKEN_SEPARATOR_REGEX = "_";
    public static final int RECORD_KEY = 0;
    //
    // separators
    //
    public static final char TOKEN_RANK_SEPARATOR = '_';
    public static final char RECORD_SEPARATOR = ':';
    public static final String RECORD_SEPARATOR_REGEX = ":";
    public static final char RECORD_EXTRA_SEPARATOR = ';';
    public static final String RECORD_EXTRA_SEPARATOR_REGEX = ";";
    public static final char RIDPAIRS_SEPARATOR = ' ';
    public static final String RIDPAIRS_SEPARATOR_REGEX = " ";
}
