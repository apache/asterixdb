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

import org.apache.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;

/**
 * Creates a JSON serialization of the lock table of the ConcurrentLockManager organized by resource. I.e. the
 * serialization will contain all resources for which lock request are recorded in the table - along with a list of
 * the requests for each resource.
 *
 * @see ConcurrentLockManager
 */
public class ResourceTablePrinter implements TablePrinter {
    private ResourceGroupTable table;
    private ResourceArenaManager resArenaMgr;
    private RequestArenaManager reqArenaMgr;
    private JobArenaManager jobArenaMgr;

    ResourceTablePrinter(ResourceGroupTable table, ResourceArenaManager resArenaMgr, RequestArenaManager reqArenaMgr,
            JobArenaManager jobArenaMgr) {
        this.table = table;
        this.resArenaMgr = resArenaMgr;
        this.reqArenaMgr = reqArenaMgr;
        this.jobArenaMgr = jobArenaMgr;
    }

    @Override
    public StringBuilder append(StringBuilder sb) {
        table.getAllLatches();
        sb.append("[\n");
        int i = 0;
        long res = -1;
        while (res == -1 && i < table.size) {
            res = table.get(i++).firstResourceIndex.get();
        }
        while (i < table.size) {
            sb = appendResource(sb, res);
            res = resArenaMgr.getNext(res);
            while (res == -1 && i < table.size) {
                res = table.get(i++).firstResourceIndex.get();
            }
            if (res == -1) {
                sb.append("\n");
                break;
            } else {
                sb.append(",\n");
            }
        }
        table.releaseAllLatches();
        return sb.append("]");
    }

    StringBuilder appendResource(StringBuilder sb, long res) {
        sb.append("{ \"dataset\": ").append(resArenaMgr.getDatasetId(res));
        sb.append(", \"hash\": ").append(resArenaMgr.getPkHashVal(res));
        sb.append(", \"max mode\": ").append(string(resArenaMgr.getMaxMode(res)));
        long lastHolder = resArenaMgr.getLastHolder(res);
        if (lastHolder != -1) {
            sb = appendRequests(sb.append(", \"holders\": "), lastHolder);
        }
        long firstUpgrader = resArenaMgr.getFirstUpgrader(res);
        if (firstUpgrader != -1) {
            sb = appendRequests(sb.append(", \"upgraders\": "), firstUpgrader);
        }
        long firstWaiter = resArenaMgr.getFirstWaiter(res);
        if (firstWaiter != -1) {
            sb = appendRequests(sb.append(", \"waiters\": "), firstWaiter);
        }
        return sb.append(" }");
    }

    StringBuilder appendRequests(StringBuilder sb, long req) {
        sb.append("[ ");
        while (req != -1) {
            appendRequest(sb, req);
            req = reqArenaMgr.getNextRequest(req);
            sb.append(req == -1 ? " ]" : ", ");
        }
        return sb;
    }

    StringBuilder appendRequest(StringBuilder sb, long req) {
        long job = reqArenaMgr.getJobSlot(req);
        sb.append("{ \"job\": ").append(jobArenaMgr.getTxnId(job));
        sb.append(", \"mode\": \"").append(string(reqArenaMgr.getLockMode(req)));
        return sb.append("\" }");
    }

    private static final String string(int lockMode) {
        return LockMode.toString((byte) lockMode);
    }
}
