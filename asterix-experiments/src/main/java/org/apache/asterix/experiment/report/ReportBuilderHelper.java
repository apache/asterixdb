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

package org.apache.asterix.experiment.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ReportBuilderHelper {

    public static void main(String[] args) throws Exception {
        String line = "INFO: DataGen[13][During ingestion only][TimeToInsert100000] 10651 in milliseconds";
        System.out.println(getEndIndexOf("DataGen[1][During ingestion only][TimeToInsert100000] 10651 in milliseconds",
                "DataGen["));
        System.out.println(getLong(line, "[TimeToInsert100000]", "in"));
        System.out.println(getLong(line, "DataGen[", "]"));
        //SIE1AReportBuilder rb = new SIE1AReportBuilder("/Users/kisskys/workspace/asterix_experiment/run-log/run-log-backup/log-1435560604069/run.log");
        //System.out.println(rb.getInstantaneousInsertPS());
    }

    public static int getEndIndexOf(String target, String pattern) {
        //get the end index of the pattern string in target string.
        int index = target.indexOf(pattern);
        if (index != -1) {
            return target.indexOf(pattern) + pattern.length();
        }
        return -1;
    }

    public static long getLong(String line, String beginPattern, String endPattern) {
        int idBeginIdx = getEndIndexOf(line, beginPattern);
        int idEndIdx = line.indexOf(endPattern, idBeginIdx);
        return Long.parseLong(line.substring(idBeginIdx, idEndIdx).trim());
    }

    public static int getInt(String line, String beginPattern, String endPattern) {
        int idBeginIdx = getEndIndexOf(line, beginPattern);
        int idEndIdx = line.indexOf(endPattern, idBeginIdx);
        return Integer.parseInt(line.substring(idBeginIdx, idEndIdx).trim());
    }

    public static double getDouble(String line, String beginPattern, String endPattern) {
        int idBeginIdx = getEndIndexOf(line, beginPattern);
        int idEndIdx = line.indexOf(endPattern, idBeginIdx);
        return Double.parseDouble(line.substring(idBeginIdx, idEndIdx).trim());
    }

    public static String getString(String line, String beginPattern, String endPattern) {
        int idBeginIdx = getEndIndexOf(line, beginPattern);
        int idEndIdx = line.indexOf(endPattern, idBeginIdx);
        return line.substring(idBeginIdx, idEndIdx).trim();
    }
    
    public static String getString(String line, String endPattern) {
        int idEndIdx = line.indexOf(endPattern, 0);
        return line.substring(0, idEndIdx).trim();
    }

    public static long getTimeStampAsLong(String line, SimpleDateFormat format) throws ParseException {
        //Jul 09, 2015 11:58:08
        //String line = "Jul 09, 2015 11:58:09 PM org.apache.hyracks.storage.am.lsm.common.impls.LSMHarness flush";
        //DateFormat format;
        //format = new SimpleDateFormat("MMM dd, yyyy hh:mm:ss");
        Date parseDate = format.parse(line);
        return parseDate.getTime();
    }
    
    protected static FileOutputStream openOutputFile(String filepath) throws IOException {
        File file = new File(filepath);
        if (file.exists()) {
            //throw new IOException(filepath + "already exists");
            file.delete();
        }
        file.createNewFile();
        return new FileOutputStream(file);
    }

    protected static void closeOutputFile(FileOutputStream fos) throws IOException {
        fos.flush();
        fos.close();
        fos = null;
    }

}
