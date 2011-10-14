/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.dataflow.common.data.comparators;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.dataflow.common.data.util.StringUtils;

public class UTF8StringBinaryComparator implements IBinaryComparator {
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        int utflen1 = StringUtils.getUTFLen(b1, s1);
        int utflen2 = StringUtils.getUTFLen(b2, s2);

        int c1 = 0;
        int c2 = 0;

        int s1Start = s1 + 2;
        int s2Start = s2 + 2;

        while (c1 < utflen1 && c2 < utflen2) {
            char ch1 = StringUtils.charAt(b1, s1Start + c1);
            char ch2 = StringUtils.charAt(b2, s2Start + c2);

            if (ch1 != ch2) {
                return ch1 - ch2;
            }
            c1 += StringUtils.charSize(b1, s1Start + c1);
            c2 += StringUtils.charSize(b2, s2Start + c2);
        }
        return utflen1 - utflen2;
    }
}
