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
package org.apache.asterix.column.assembler;

import static org.junit.Assert.assertEquals;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.junit.Test;

/**
 * {@link MapValueAssembler} folds the reports of its key and value arms into one per round. A value arm whose column
 * is absent from the current leaf reports an ancestor-level missing on every round, entry rounds included; counting
 * those reports rather than rounds filled the count after a few entries and forwarded an ancestor event that never
 * happened.
 */
public class MapValueAssemblerRoundTest {
    private static final int MAP_LEVEL = 2;

    /** Stands in for the object holding the map: counts what reaches it from the map. */
    private static final class Parent extends AbstractNestedValueAssembler {
        int ancestorMissing;
        int missing;

        Parent() {
            super(MAP_LEVEL - 1, new AssemblerInfo());
        }

        @Override
        void addValue(AbstractValueAssembler value) {
        }

        @Override
        void addNull(AbstractValueAssembler value) {
        }

        @Override
        void addMissing() {
            missing++;
        }

        @Override
        void addOpenField(IValueReference taggedFieldName, IValueReference value) {
        }

        @Override
        void addMissingToAncestor(int missingLevel) {
            ancestorMissing++;
        }

        @Override
        void addNullToAncestor(int nullLevel) {
        }

        @Override
        void addValueToParent() {
        }

        @Override
        public IValueReference getValue() {
            return null;
        }
    }

    /** The key column's assembler: always holds the tagged string "k". */
    private static final class Key extends AbstractValueAssembler {
        private final VoidPointable value = new VoidPointable();

        Key(MapValueAssembler map) {
            super(MAP_LEVEL + 1, new AssemblerInfo(BuiltinType.ASTRING, map, false));
            value.set(new byte[] { ATypeTag.STRING.serialize(), 1, 'k' }, 0, 3);
        }

        @Override
        void addNullToAncestor(int nullLevel) {
        }

        @Override
        void addMissingToAncestor(int missingLevel) {
        }

        @Override
        void addValueToParent() {
        }

        @Override
        public IValueReference getValue() {
            return value;
        }
    }

    @Test
    public void anAbsentArmDoesNotTurnEntryRoundsIntoAnAncestorEvent() throws Exception {
        Parent parent = new Parent();
        MapValueAssembler map = new MapValueAssembler(MAP_LEVEL, new AssemblerInfo(BuiltinType.ANY, parent, false), 0);
        Key key = new Key(map);
        map.setKeyAssembler(key);
        // The key, and one value arm whose column this leaf does not have.
        map.setReporters(2);
        map.start();

        for (int entry = 0; entry < 3; entry++) {
            map.addValue(key);
            map.addMissingToAncestor(0);
        }
        assertEquals("entry rounds must not reach the parent as an ancestor event", 0,
                parent.ancestorMissing + parent.missing);

        // A real ancestor event: every reporter says the map itself is missing.
        map.addMissingToAncestor(0);
        map.addMissingToAncestor(0);
        assertEquals("an ancestor event reaches the parent exactly once", 1, parent.ancestorMissing);
    }
}
