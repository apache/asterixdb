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

import org.apache.hadoop.io.ByteWritable;

import edu.uci.ics.hivesterix.serde.lazy.objectinspector.primitive.LazyByteObjectInspector;

/**
 * LazyObject for storing a value of Byte.
 * <p>
 * Part of the code is adapted from Apache Harmony Project. As with the specification, this implementation relied on code laid out in <a href="http://www.hackersdelight.org/">Henry S. Warren, Jr.'s Hacker's Delight, (Addison Wesley, 2002)</a> as well as <a href="http://aggregate.org/MAGIC/">The Aggregate's Magic Algorithms</a>.
 * </p>
 */
public class LazyByte extends LazyPrimitive<LazyByteObjectInspector, ByteWritable> {

    public LazyByte(LazyByteObjectInspector oi) {
        super(oi);
        data = new ByteWritable();
    }

    public LazyByte(LazyByte copy) {
        super(copy);
        data = new ByteWritable(copy.data.get());
    }

    @Override
    public void init(byte[] bytes, int start, int length) {
        if (length == 0) {
            isNull = true;
            return;
        } else
            isNull = false;

        assert (1 == length);
        data.set(bytes[start]);
    }

}
