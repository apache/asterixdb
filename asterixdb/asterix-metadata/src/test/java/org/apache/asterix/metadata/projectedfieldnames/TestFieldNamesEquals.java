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
package org.apache.asterix.metadata.projectedfieldnames;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.metadata.declared.ExternalDataProjectionInfo;
import org.junit.Assert;
import org.junit.Test;

public class TestFieldNamesEquals {

    @Test
    public void testEqual() {
        ExternalDataProjectionInfo p1 = new ExternalDataProjectionInfo();
        ExternalDataProjectionInfo p2 = new ExternalDataProjectionInfo();
        setFieldNames(p1, new String[] { "a.b.c", "d.e.f", "g.h.i" });
        setFieldNames(p2, new String[] { "a.b.c", "d.e.f", "g.h.i" });
        Assert.assertEquals(p1, p2);
    }

    @Test
    public void testReversed() {
        ExternalDataProjectionInfo p1 = new ExternalDataProjectionInfo();
        ExternalDataProjectionInfo p2 = new ExternalDataProjectionInfo();
        setFieldNames(p1, new String[] { "a.b.c", "d.e.f", "g.h.i" });
        setFieldNames(p2, new String[] { "g.h.i", "d.e.f", "a.b.c" });
        Assert.assertEquals(p1, p2);
    }

    @Test
    public void testDifferentPermutations() {
        ExternalDataProjectionInfo p1 = new ExternalDataProjectionInfo();
        ExternalDataProjectionInfo p2 = new ExternalDataProjectionInfo();
        setFieldNames(p1, new String[] { "a.b.c", "d.e.f", "g.h.i" });
        setFieldNames(p2, new String[] { "d.e.f", "g.h.i", "a.b.c" });
        Assert.assertEquals(p1, p2);

        p1 = new ExternalDataProjectionInfo();
        p2 = new ExternalDataProjectionInfo();
        setFieldNames(p1, new String[] { "a.b.c", "d.e.f", "g.h.i" });
        setFieldNames(p2, new String[] { "g.h.i", "d.e.f", "a.b.c" });
        Assert.assertEquals(p1, p2);

        p1 = new ExternalDataProjectionInfo();
        p2 = new ExternalDataProjectionInfo();
        setFieldNames(p1, new String[] { "a.b.c", "d.e.f", "g.h.i" });
        setFieldNames(p2, new String[] { "g.h.i", "a.b.c", "d.e.f" });
        Assert.assertEquals(p1, p2);
    }

    @Test
    public void testDifferentLengths() {
        ExternalDataProjectionInfo p1 = new ExternalDataProjectionInfo();
        ExternalDataProjectionInfo p2 = new ExternalDataProjectionInfo();
        setFieldNames(p1, new String[] { "a.b.c", "d.e.f" });
        setFieldNames(p2, new String[] { "d.e.f", "a.b.c", "g" });
        Assert.assertNotEquals(p1, p2);
    }

    @Test
    public void testEqualSubPath() {
        ExternalDataProjectionInfo p1 = new ExternalDataProjectionInfo();
        ExternalDataProjectionInfo p2 = new ExternalDataProjectionInfo();
        setFieldNames(p1, new String[] { "a.b.c", "d.e.f.g" });
        setFieldNames(p2, new String[] { "d.e.f", "a.b.c" });
        Assert.assertNotEquals(p1, p2);
    }

    private static void setFieldNames(ExternalDataProjectionInfo p, String[] fieldNames) {
        List<List<String>> fieldNamesList = p.getProjectionInfo();
        for (String fnString : fieldNames) {
            List<String> fnList = new ArrayList<>();
            String[] fn = fnString.split("[.]");
            Collections.addAll(fnList, fn);
            fieldNamesList.add(fnList);
        }
    }

}
