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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import edu.uci.ics.hivesterix.serde.lazy.objectinspector.LazyColumnarObjectInspector;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * LazyObject for storing a struct. The field of a struct can be primitive or
 * non-primitive.
 * LazyStruct does not deal with the case of a NULL struct. That is handled by
 * the parent LazyObject.
 */
@SuppressWarnings("rawtypes")
public class LazyColumnar extends LazyNonPrimitive<LazyColumnarObjectInspector> {

    /**
     * IFrameTupleReference: the backend of the struct
     */
    IFrameTupleReference tuple;

    /**
     * Whether the data is already parsed or not.
     */
    boolean reset;

    /**
     * The fields of the struct.
     */
    LazyObject[] fields;

    /**
     * Whether init() has been called on the field or not.
     */
    boolean[] fieldVisited;

    /**
     * whether it is the first time initialization
     */
    boolean start = true;

    /**
     * Construct a LazyStruct object with the ObjectInspector.
     */
    public LazyColumnar(LazyColumnarObjectInspector oi) {
        super(oi);
    }

    /**
     * Set the row data for this LazyStruct.
     * 
     * @see LazyObject#init(ByteArrayRef, int, int)
     */
    @Override
    public void init(byte[] bytes, int start, int length) {
        super.init(bytes, start, length);
        reset = false;
    }

    /**
     * Parse the byte[] and fill each field.
     */
    private void parse() {

        if (start) {
            // initialize field array and reusable objects
            List<? extends StructField> fieldRefs = ((StructObjectInspector) oi).getAllStructFieldRefs();

            fields = new LazyObject[fieldRefs.size()];
            for (int i = 0; i < fields.length; i++) {
                fields[i] = LazyFactory.createLazyObject(fieldRefs.get(i).getFieldObjectInspector());
            }
            fieldVisited = new boolean[fields.length];
            start = false;
        }

        Arrays.fill(fieldVisited, false);
        reset = true;
    }

    /**
     * Get one field out of the struct.
     * If the field is a primitive field, return the actual object. Otherwise
     * return the LazyObject. This is because PrimitiveObjectInspector does not
     * have control over the object used by the user - the user simply directly
     * use the Object instead of going through Object
     * PrimitiveObjectInspector.get(Object).
     * 
     * @param fieldID
     *            The field ID
     * @return The field as a LazyObject
     */
    public Object getField(int fieldID) {
        if (!reset) {
            parse();
        }
        return uncheckedGetField(fieldID);
    }

    /**
     * Get the field out of the row without checking parsed. This is called by
     * both getField and getFieldsAsList.
     * 
     * @param fieldID
     *            The id of the field starting from 0.
     * @param nullSequence
     *            The sequence representing NULL value.
     * @return The value of the field
     */
    private Object uncheckedGetField(int fieldID) {
        // get the buffer
        byte[] buffer = tuple.getFieldData(fieldID);
        // get the offset of the field
        int s1 = tuple.getFieldStart(fieldID);
        int l1 = tuple.getFieldLength(fieldID);

        if (!fieldVisited[fieldID]) {
            fieldVisited[fieldID] = true;
            fields[fieldID].init(buffer, s1, l1);
        }
        // if (fields[fieldID].getObject() == null) {
        // throw new IllegalStateException("illegal field " + fieldID);
        // }
        return fields[fieldID].getObject();
    }

    ArrayList<Object> cachedList;

    /**
     * Get the values of the fields as an ArrayList.
     * 
     * @return The values of the fields as an ArrayList.
     */
    public ArrayList<Object> getFieldsAsList() {
        if (!reset) {
            parse();
        }
        if (cachedList == null) {
            cachedList = new ArrayList<Object>();
        } else {
            cachedList.clear();
        }
        for (int i = 0; i < fields.length; i++) {
            cachedList.add(uncheckedGetField(i));
        }
        return cachedList;
    }

    @Override
    public Object getObject() {
        return this;
    }

    protected boolean getParsed() {
        return reset;
    }

    protected void setParsed(boolean parsed) {
        this.reset = parsed;
    }

    protected LazyObject[] getFields() {
        return fields;
    }

    protected void setFields(LazyObject[] fields) {
        this.fields = fields;
    }

    protected boolean[] getFieldInited() {
        return fieldVisited;
    }

    protected void setFieldInited(boolean[] fieldInited) {
        this.fieldVisited = fieldInited;
    }

    /**
     * rebind a frametuplereference to the struct
     */
    public void init(IFrameTupleReference r) {
        this.tuple = r;
        reset = false;
    }
}
