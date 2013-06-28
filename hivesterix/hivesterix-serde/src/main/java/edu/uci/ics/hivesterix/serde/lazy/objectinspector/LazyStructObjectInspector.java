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
package edu.uci.ics.hivesterix.serde.lazy.objectinspector;

import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

import edu.uci.ics.hivesterix.serde.lazy.LazyStruct;

/**
 * ObjectInspector for LazyStruct.
 * 
 * @see LazyStruct
 */
public class LazyStructObjectInspector extends StandardStructObjectInspector {

    protected LazyStructObjectInspector(List<String> structFieldNames, List<ObjectInspector> structFieldObjectInspectors) {
        super(structFieldNames, structFieldObjectInspectors);
    }

    protected LazyStructObjectInspector(List<StructField> fields) {
        super(fields);
    }

    @Override
    public Object getStructFieldData(Object data, StructField fieldRef) {
        if (data == null) {
            return null;
        }
        LazyStruct struct = (LazyStruct) data;
        MyField f = (MyField) fieldRef;

        int fieldID = f.getFieldID();
        assert (fieldID >= 0 && fieldID < fields.size());

        return struct.getField(fieldID);
    }

    @Override
    public List<Object> getStructFieldsDataAsList(Object data) {
        if (data == null) {
            return null;
        }
        LazyStruct struct = (LazyStruct) data;
        return struct.getFieldsAsList();
    }
}
