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
package org.apache.asterix.translator;

import java.io.Serializable;

public class ResultProperties implements Serializable {
    private static final long serialVersionUID = -4741260459407538017L;

    public static final long DEFAULT_MAX_READS = 1;
    public static final long DEFAULT_RESULT_TTL = -1L; // -1 means use system default
    private final IStatementExecutor.ResultDelivery delivery;
    private final long maxReads;
    private final long resultTtlInMillis;

    public ResultProperties(IStatementExecutor.ResultDelivery delivery) {
        this(delivery, DEFAULT_MAX_READS, DEFAULT_RESULT_TTL);
    }

    public ResultProperties(IStatementExecutor.ResultDelivery delivery, long maxReads) {
        this(delivery, maxReads, DEFAULT_RESULT_TTL);
    }

    public ResultProperties(IStatementExecutor.ResultDelivery delivery, long maxReads, long resultTtlInMillis) {
        this.delivery = delivery;
        this.maxReads = maxReads;
        this.resultTtlInMillis = resultTtlInMillis;
    }

    public IStatementExecutor.ResultDelivery getDelivery() {
        return delivery;
    }

    public long getMaxReads() {
        return maxReads;
    }

    public long getResultTtlInMillis() {
        return resultTtlInMillis;
    }

    public ResultProperties getNcToCcResultProperties() {
        if (delivery != IStatementExecutor.ResultDelivery.IMMEDIATE) {
            return this;
        }
        // switch IMMEDIATE to DEFERRED since the result will be severed by the NC
        return new ResultProperties(IStatementExecutor.ResultDelivery.DEFERRED, maxReads, resultTtlInMillis);
    }
}
