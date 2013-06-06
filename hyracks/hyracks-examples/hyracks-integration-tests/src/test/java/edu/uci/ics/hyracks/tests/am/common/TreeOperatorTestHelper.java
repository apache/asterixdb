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

package edu.uci.ics.hyracks.tests.am.common;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TreeOperatorTestHelper implements ITreeIndexOperatorTestHelper {

    protected final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    protected final String sep = System.getProperty("file.separator");

    public String getPrimaryIndexName() {
        return System.getProperty("java.io.tmpdir") + sep + "primary" + simpleDateFormat.format(new Date());
    }

    public String getSecondaryIndexName() {
        return System.getProperty("java.io.tmpdir") + sep + "secondary" + simpleDateFormat.format(new Date());
    }

    @Override
    public void cleanup(String primaryFileName, String secondaryFileName) {
        File primary = new File(primaryFileName);
        if (primary.exists()) {
            primary.deleteOnExit();
        }
        File secondary = new File(secondaryFileName);
        if (secondary.exists()) {
            secondary.deleteOnExit();
        }
    }
}
