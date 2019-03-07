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
package org.apache.asterix.common.transactions;

public class LogConstants {

    public static final int CHKSUM_LEN = Long.BYTES;
    public static final int FLDCNT_LEN = Integer.BYTES;
    public static final int DS_LEN = Integer.BYTES;
    public static final int LOG_SOURCE_LEN = Byte.BYTES;
    public static final int LOGRCD_SZ_LEN = Integer.BYTES;
    public static final int NEWOP_LEN = Byte.BYTES;
    public static final int NEWVALSZ_LEN = Integer.BYTES;
    public static final int PKHASH_LEN = Integer.BYTES;
    public static final int PKSZ_LEN = Integer.BYTES;
    public static final int PRVLSN_LEN = Long.BYTES;
    public static final int RS_PARTITION_LEN = Integer.BYTES;
    public static final int RSID_LEN = Long.BYTES;
    public static final int SEQ_NUM_LEN = Long.BYTES;
    public static final int TYPE_LEN = Byte.BYTES;
    public static final int UUID_LEN = Long.BYTES;
    public static final int FLUSHING_COMPONENT_MINID_LEN = Long.BYTES;
    public static final int FLUSHING_COMPONENT_MAXID_LEN = Long.BYTES;

    public static final int ALL_RECORD_HEADER_LEN = LOG_SOURCE_LEN + TYPE_LEN + TxnId.BYTES;
    public static final int ENTITY_RESOURCE_HEADER_LEN = RS_PARTITION_LEN + DatasetId.BYTES;
    public static final int ENTITY_VALUE_HEADER_LEN = PKHASH_LEN + PKSZ_LEN;
    public static final int UPDATE_LSN_HEADER = RSID_LEN + LOGRCD_SZ_LEN;
    public static final int UPDATE_BODY_HEADER = FLDCNT_LEN + NEWOP_LEN + NEWVALSZ_LEN;

    public static final int JOB_TERMINATE_LOG_SIZE = ALL_RECORD_HEADER_LEN + CHKSUM_LEN;
    public static final int ENTITY_COMMIT_LOG_BASE_SIZE =
            ALL_RECORD_HEADER_LEN + ENTITY_RESOURCE_HEADER_LEN + ENTITY_VALUE_HEADER_LEN + CHKSUM_LEN;
    public static final int UPDATE_LOG_BASE_SIZE = ENTITY_COMMIT_LOG_BASE_SIZE + UPDATE_LSN_HEADER + UPDATE_BODY_HEADER;
    public static final int FILTER_LOG_BASE_SIZE =
            ALL_RECORD_HEADER_LEN + ENTITY_RESOURCE_HEADER_LEN + UPDATE_BODY_HEADER + UPDATE_LSN_HEADER + CHKSUM_LEN;
    public static final int FLUSH_LOG_SIZE = ALL_RECORD_HEADER_LEN + DS_LEN + RS_PARTITION_LEN
            + FLUSHING_COMPONENT_MINID_LEN + FLUSHING_COMPONENT_MAXID_LEN + CHKSUM_LEN;
    public static final int WAIT_LOG_SIZE = ALL_RECORD_HEADER_LEN + CHKSUM_LEN;
    public static final int MARKER_BASE_LOG_SIZE =
            ALL_RECORD_HEADER_LEN + CHKSUM_LEN + DS_LEN + RS_PARTITION_LEN + PRVLSN_LEN + LOGRCD_SZ_LEN;

    public static final int V_0 = 0;
    public static final int V_1 = 1;
    public static final int V_CURRENT = V_1;
    public static final int VERSION_MIN = 0;
    public static final int VERSION_MAX = (0xff >> 2) - 1;

    public static final int LOG_SOURCE_MIN = 0;
    public static final int LOG_SOURCE_MAX = (1 << 2) - 1;

    private LogConstants() {
    }
}
