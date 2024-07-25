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
package org.apache.hyracks.algebricks.core.algebra.base;

public interface OperatorAnnotations {
    // hints
    String USE_HASH_GROUP_BY = "USE_HASH_GROUP_BY"; // -->
    String USE_EXTERNAL_GROUP_BY = "USE_EXTERNAL_GROUP_BY"; // -->
    String USE_STATIC_RANGE = "USE_STATIC_RANGE"; // -->
    String USE_DYNAMIC_RANGE = "USE_DYNAMIC_RANGE";
    // Boolean
    String CARDINALITY = "CARDINALITY"; // -->
    // Integer
    String MAX_NUMBER_FRAMES = "MAX_NUMBER_FRAMES"; // -->
    // Integer
    String OP_INPUT_CARDINALITY = "INPUT_CARDINALITY";
    String OP_OUTPUT_CARDINALITY = "OUTPUT_CARDINALITY";
    String OP_COST_TOTAL = "TOTAL_COST";
    String OP_COST_LOCAL = "OP_COST";
    String OP_LEFT_EXCHANGE_COST = "LEFT_EXCHANGE_COST";
    String OP_RIGHT_EXCHANGE_COST = "RIGHT_EXCHANGE_COST";
}
