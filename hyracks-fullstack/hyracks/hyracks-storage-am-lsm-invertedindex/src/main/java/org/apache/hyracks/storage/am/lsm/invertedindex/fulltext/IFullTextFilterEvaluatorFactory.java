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

package org.apache.hyracks.storage.am.lsm.invertedindex.fulltext;

import java.io.Serializable;

import org.apache.hyracks.api.io.IJsonSerializable;

// This full-text filter evaluator factory would to be stored in the index local resource,
// so it needs to be IJsonSerializable.
// Also, it would to be distributed from CC (compile-time) to NC (run-time), so it needs to be Serializable.
//
// Such a IFullTextFilterEvaluatorFactory should always be wrapped in a IFullTextConfigEvaluatorFactory
// because filter cannot live without a config: a full-text config is responsible to tokenize strings
// and then feed the tokens into the filters.
public interface IFullTextFilterEvaluatorFactory extends IJsonSerializable, Serializable {
    IFullTextFilterEvaluator createFullTextFilterEvaluator();
}
