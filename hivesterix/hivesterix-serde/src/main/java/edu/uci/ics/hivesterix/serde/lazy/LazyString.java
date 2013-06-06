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

import org.apache.hadoop.io.Text;

import edu.uci.ics.hivesterix.serde.lazy.LazyUtils.VInt;
import edu.uci.ics.hivesterix.serde.lazy.objectinspector.primitive.LazyStringObjectInspector;

/**
 * LazyObject for storing a value of String.
 */
public class LazyString extends LazyPrimitive<LazyStringObjectInspector, Text> {

    public LazyString(LazyStringObjectInspector oi) {
        super(oi);
        data = new Text();
    }

    public LazyString(LazyString copy) {
        super(copy);
        data = new Text(copy.data);
    }

    VInt vInt = new LazyUtils.VInt();

    @Override
    public void init(byte[] bytes, int start, int length) {
        if (length == 0) {
            isNull = true;
            return;
        } else
            isNull = false;

        // get the byte length of the string
        LazyUtils.readVInt(bytes, start, vInt);
        if (vInt.value + vInt.length != length)
            throw new IllegalStateException("parse string: length mismatch, expected " + (vInt.value + vInt.length)
                    + " but get " + length);
        assert (length - vInt.length > -1);
        data.set(bytes, start + vInt.length, length - vInt.length);
    }

}
