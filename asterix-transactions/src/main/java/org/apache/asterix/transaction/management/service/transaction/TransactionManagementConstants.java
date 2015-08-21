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
package edu.uci.ics.asterix.transaction.management.service.transaction;

/**
 * Represents a umbrella class containing constant that are used by transaction
 * sub-systems (Lock/Log)Manager.
 */
public class TransactionManagementConstants {

    public static class ResourceMgrIds {
        public static final byte BTREE_RESOURCE_MGR_ID = 1;
        public static final byte METADATA_RESOURCE_MGR_ID = 2;
    }

    public static class LogManagerConstants {
        public static final int TERMINAL_LSN = -1;
    }

    public static class LockManagerConstants {
        public static class LockMode {
            public static final byte ANY = -1;
            public static final byte NL  =  0;
            public static final byte IS  =  1;
            public static final byte IX  =  2;
            public static final byte S   =  3;
            public static final byte X   =  4;
            
            public static byte intentionMode(byte mode) {
                switch (mode) {
                    case S:  return IS;
                    case X:  return IX;
                    default: throw new IllegalArgumentException(
                            "no intention lock mode for " + toString(mode));
                }                
            }
            
            public static String toString(byte mode) {
                switch (mode) {
                    case ANY: return "ANY";
                    case NL:  return "NL";
                    case IS:  return "IS";
                    case IX:  return "IX";
                    case S:   return "S";
                    case X:   return "X";
                    default:  throw new IllegalArgumentException("no such lock mode");
                }
            }
        }
    }
}
