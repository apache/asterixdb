///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//package org.apache.asterix.om.values.writer.filters;
//
//public class LongRowFilterWriter extends AbstractRowFilterWriter {
//    private long min;
//    private long max;
//
//    public LongRowFilterWriter() {
//        reset();
//    }
//
//    @Override
//    public void addLong(long value) {
//        min = Math.min(min, value);
//        max = Math.max(max, value);
//    }
//
//    @Override
//    public long getMinNormalizedValue() {
//        return min;
//    }
//
//    @Override
//    public long getMaxNormalizedValue() {
//        return max;
//    }
//
//    @Override
//    public void reset() {
//        min = Long.MAX_VALUE;
//        max = Long.MIN_VALUE;
//    }
//}
