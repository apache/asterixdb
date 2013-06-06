/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hivesterix.serde.lazy;

import org.apache.hadoop.io.DoubleWritable;

import edu.uci.ics.hivesterix.serde.lazy.objectinspector.primitive.LazyDoubleObjectInspector;

/**
 * LazyObject for storing a value of Double.
 */
public class LazyDouble extends LazyPrimitive<LazyDoubleObjectInspector, DoubleWritable> {

    public LazyDouble(LazyDoubleObjectInspector oi) {
        super(oi);
        data = new DoubleWritable();
    }

    public LazyDouble(LazyDouble copy) {
        super(copy);
        data = new DoubleWritable(copy.data.get());
    }

    @Override
    public void init(byte[] bytes, int start, int length) {
        if (length == 0) {
            isNull = true;
            return;
        } else
            isNull = false;
        assert (8 == length);
        data.set(Double.longBitsToDouble(LazyUtils.byteArrayToLong(bytes, start)));
    }

}
