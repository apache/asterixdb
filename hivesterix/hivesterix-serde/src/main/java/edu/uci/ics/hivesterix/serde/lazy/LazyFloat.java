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

import org.apache.hadoop.io.FloatWritable;

import edu.uci.ics.hivesterix.serde.lazy.objectinspector.primitive.LazyFloatObjectInspector;

/**
 * LazyObject for storing a value of Double.
 */
public class LazyFloat extends LazyPrimitive<LazyFloatObjectInspector, FloatWritable> {

    public LazyFloat(LazyFloatObjectInspector oi) {
        super(oi);
        data = new FloatWritable();
    }

    public LazyFloat(LazyFloat copy) {
        super(copy);
        data = new FloatWritable(copy.data.get());
    }

    @Override
    public void init(byte[] bytes, int start, int length) {
        if (length == 0) {
            isNull = true;
            return;
        } else
            isNull = false;

        assert (4 == length);
        data.set(Float.intBitsToFloat(LazyUtils.byteArrayToInt(bytes, start)));
    }

}
