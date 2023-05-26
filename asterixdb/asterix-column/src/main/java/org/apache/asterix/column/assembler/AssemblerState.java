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

import org.apache.hyracks.storage.am.lsm.btree.column.error.ColumnarValueException;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class AssemblerState {
    private EndOfRepeatedGroupAssembler currentGroup;

    public AssemblerState() {
        // Initially, not in a group
        currentGroup = null;
    }

    public EndOfRepeatedGroupAssembler enterRepeatedGroup(EndOfRepeatedGroupAssembler newGroup) {
        EndOfRepeatedGroupAssembler previousGroup = currentGroup;
        currentGroup = newGroup;
        return previousGroup;
    }

    public boolean isCurrentGroup(EndOfRepeatedGroupAssembler group) {
        return currentGroup == group;
    }

    public void exitRepeatedGroup(EndOfRepeatedGroupAssembler previousGroup) {
        currentGroup = previousGroup;
    }

    public boolean isInGroup() {
        return currentGroup != null;
    }

    public void appendStateInfo(ColumnarValueException e) {
        ObjectNode stateNode = e.createNode(getClass().getSimpleName());
        if (isInGroup()) {
            stateNode.put("inGroup", true);
            currentGroup.reader.appendReaderInformation(stateNode.putObject("endOfGroupReader"));
        } else {
            stateNode.put("inGroup", false);
        }
    }
}
