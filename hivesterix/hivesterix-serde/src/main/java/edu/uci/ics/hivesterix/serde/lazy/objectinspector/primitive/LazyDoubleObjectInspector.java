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
package edu.uci.ics.hivesterix.serde.lazy.objectinspector.primitive;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.DoubleWritable;

import edu.uci.ics.hivesterix.serde.lazy.LazyDouble;

/**
 * A WritableDoubleObjectInspector inspects a DoubleWritable Object.
 */
public class LazyDoubleObjectInspector extends AbstractPrimitiveLazyObjectInspector<DoubleWritable> implements
        DoubleObjectInspector {

    LazyDoubleObjectInspector() {
        super(PrimitiveObjectInspectorUtils.doubleTypeEntry);
    }

    @Override
    public double get(Object o) {
        return getPrimitiveWritableObject(o).get();
    }

    @Override
    public Object copyObject(Object o) {
        return o == null ? null : new LazyDouble((LazyDouble) o);
    }

    @Override
    public Object getPrimitiveJavaObject(Object o) {
        return o == null ? null : Double.valueOf(get(o));
    }
}
