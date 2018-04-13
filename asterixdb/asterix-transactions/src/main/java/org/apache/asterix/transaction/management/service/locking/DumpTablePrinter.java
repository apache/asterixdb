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

package org.apache.asterix.transaction.management.service.locking;

import it.unimi.dsi.fastutil.longs.Long2LongMap;

public class DumpTablePrinter implements TablePrinter {
    private ResourceGroupTable table;
    private ResourceArenaManager resArenaMgr;
    private RequestArenaManager reqArenaMgr;
    private JobArenaManager jobArenaMgr;
    private Long2LongMap txnIdToJobSlotMap;

    DumpTablePrinter(ResourceGroupTable table, ResourceArenaManager resArenaMgr, RequestArenaManager reqArenaMgr,
            JobArenaManager jobArenaMgr, Long2LongMap txnIdToJobSlotMap) {
        this.table = table;
        this.resArenaMgr = resArenaMgr;
        this.reqArenaMgr = reqArenaMgr;
        this.jobArenaMgr = jobArenaMgr;
        this.txnIdToJobSlotMap = txnIdToJobSlotMap;
    }

    public StringBuilder append(StringBuilder sb) {
        table.getAllLatches();
        try {
            sb.append(">>dump_begin\t>>----- [resTable] -----\n");
            table.append(sb);
            sb.append(">>dump_end\t>>----- [resTable] -----\n");

            sb.append(">>dump_begin\t>>----- [resArenaMgr] -----\n");
            resArenaMgr.append(sb);
            sb.append(">>dump_end\t>>----- [resArenaMgr] -----\n");

            sb.append(">>dump_begin\t>>----- [reqArenaMgr] -----\n");
            reqArenaMgr.append(sb);
            sb.append(">>dump_end\t>>----- [reqArenaMgr] -----\n");

            sb.append(">>dump_begin\t>>----- [txnIdSlotMap] -----\n");
            for (Long i : txnIdToJobSlotMap.keySet()) {
                sb.append(i).append(" : ");
                TypeUtil.Global.append(sb, txnIdToJobSlotMap.get(i));
                sb.append("\n");
            }
            sb.append(">>dump_end\t>>----- [jobIdSlotMap] -----\n");

            sb.append(">>dump_begin\t>>----- [jobArenaMgr] -----\n");
            jobArenaMgr.append(sb);
            sb.append(">>dump_end\t>>----- [jobArenaMgr] -----\n");
        } finally {
            table.releaseAllLatches();
        }
        return sb;
    }

    String resQueueToString(long resSlot) {
        return appendResQueue(new StringBuilder(), resSlot).toString();
    }

    StringBuilder appendResQueue(StringBuilder sb, long resSlot) {
        resArenaMgr.appendRecord(sb, resSlot);
        sb.append("\n");
        appendReqQueue(sb, resArenaMgr.getLastHolder(resSlot));
        return sb;
    }

    StringBuilder appendReqQueue(StringBuilder sb, long head) {
        while (head != -1) {
            reqArenaMgr.appendRecord(sb, head);
            sb.append("\n");
            head = reqArenaMgr.getNextRequest(head);
        }
        return sb;
    }
}
