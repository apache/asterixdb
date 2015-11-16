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
package org.apache.hyracks.algebricks.core.algebra.expressions;

public class BroadcastExpressionAnnotation implements IExpressionAnnotation {

    public static final String BROADCAST_ANNOTATION_KEY = "broadcast";

    public enum BroadcastSide {
        LEFT,
        RIGHT
    };

    private BroadcastSide side;

    @Override
    public Object getObject() {
        return side;
    }

    @Override
    public void setObject(Object side) {
        this.side = (BroadcastSide) side;
    }

    @Override
    public IExpressionAnnotation copy() {
        BroadcastExpressionAnnotation bcast = new BroadcastExpressionAnnotation();
        bcast.side = side;
        return bcast;
    }

    @Override
    public String toString() {
        return BROADCAST_ANNOTATION_KEY;
    }

}
