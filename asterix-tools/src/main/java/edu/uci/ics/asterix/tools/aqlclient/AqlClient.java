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

package edu.uci.ics.asterix.tools.aqlclient;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

/**
 * This class is to automate AQL queries for benchmarking.
 * The code is written by Pouria for the purpose of benchmarking ASTERIX.
 */
public class AqlClient {

    static ArrayList<String> qFiles;
    static ArrayList<String> qNames;

    /*
     * This code loads a set of AQL-Queries and runs them against Asterix. It
     * runs the queries for a number of iterations, specified by the user. For
     * each query it shows the Hyracks time, Asterix Time, and Client time on
     * the screen, while it also dumps its stats into an output file, to make it
     * persistent for future use/reference.
     */
    public static void main(String args[]) throws Exception {
        /*
         * Arguments: args0 - Path to the file, that contains list of query
         * files. The assumption is that each AQL query, is saved in a file, and
         * its path is mentioned in arg[0]. Each line of arg[0] file, denotes
         * one query file. args1 - IP-Address of CC args2 - Path to the output
         * file, for dumping stats args3 - Number of iterations, you want the
         * simulation to be run args4 - Set it to true, so you stats, on the
         * console, as queries are running.
         */
        if (args.length != 5) {
            System.out
                    .println("Usage: [0]=List-of-Query-Files, [1]=ccIPadr, [2]=outputFile, [3]=Iteration# , [4]=boolean-showOnConsole ");
            return;
        }
        qFiles = new ArrayList<String>();
        qNames = new ArrayList<String>();

        loadQueriesPaths(args[0]);
        String ccUrl = args[1];
        String outputFile = args[2];
        int iteration = Integer.parseInt(args[3]);
        boolean showOnConsole = Boolean.parseBoolean(args[4]);

        PrintWriter pw = new PrintWriter(new File(outputFile));
        pw.println("Code\tStatus\tName\tHyracksTime\tAsterixTime\tClientTime\n");

        DefaultHttpClient httpclient = new DefaultHttpClient();
        HttpPost httpPost = new HttpPost("http://" + ccUrl + ":19001");
        List<NameValuePair> nvps = new ArrayList<NameValuePair>();
        nvps.add(new BasicNameValuePair("hyracks-port", "1098"));
        nvps.add(new BasicNameValuePair("hyracks-ip", ccUrl));
        nvps.add(new BasicNameValuePair("display-result", "true"));
        nvps.add(new BasicNameValuePair("query", null)); // it will get its
                                                         // value in the loop
                                                         // below

        int ixToremove = nvps.size() - 1;
        try {
            for (int i = 0; i < iteration; i++) {
                for (int x = 0; x < qFiles.size(); x++) {
                    nvps.remove(ixToremove);
                    String query = readQueryFromFile(qFiles.get(x));
                    String qName = qNames.get(x);
                    System.out.println("\n\nNow Running Query " + qName + " in iteration " + i);
                    nvps.add(new BasicNameValuePair("query", query));

                    httpPost.setEntity(new UrlEncodedFormEntity(nvps));
                    long s = System.currentTimeMillis();
                    HttpResponse response = httpclient.execute(httpPost);
                    long e = System.currentTimeMillis();

                    String status = response.getStatusLine().toString();
                    HttpEntity entity = response.getEntity();
                    String content = EntityUtils.toString(entity);
                    EntityUtils.consume(entity);

                    double[] times = extractStats(content);
                    double endToend = ((double) (e - s)) / 1000.00; // This is
                                                                    // the
                                                                    // client-time
                                                                    // (end to
                                                                    // end delay
                                                                    // from
                                                                    // client's
                                                                    // perspective)
                    pw.print(status + "\t" + qName + "\t" + times[0] + "\t" + times[1] + "\t" + endToend + "\n");

                    if (showOnConsole) {
                        // System.out.println("Iteration "+i+"\n"+content+"\n");
                        // //Uncomment this line, if you want to see the whole
                        // content of the returned HTML page
                        System.out.print(qName + "\t" + status + "\t" + times[0] + "\t" + times[1] + "\t" + endToend
                                + " (iteration " + (i) + ")\n");
                    }
                }
                pw.println();
            }
        } finally {
            pw.close();
            httpPost.releaseConnection();
        }

    }

    // Assumption: It contains one query file path per line
    private static void loadQueriesPaths(String qFilesPath) throws Exception {
        BufferedReader in = new BufferedReader(new FileReader(qFilesPath));
        String str;
        while ((str = in.readLine()) != null) {
            qFiles.add(str.trim());
            int nameIx = str.lastIndexOf('/');
            qNames.add(new String(str.substring(nameIx + 1)));
        }
        in.close();
    }

    private static String readQueryFromFile(String filePath) throws Exception {
        BufferedReader in = new BufferedReader(new FileReader(filePath));
        String query = "";
        String str;
        while ((str = in.readLine()) != null) {
            query += str + "\n";
        }
        in.close();
        return query;
    }

    private static double[] extractStats(String content) {
        int hyracksTimeIx = content.indexOf("<PRE>Duration:");
        if (hyracksTimeIx < 0) {
            return new double[] { -1, -1 };
        }
        int endHyracksTimeIx = content.indexOf("</PRE>", hyracksTimeIx);
        double hTime = Double.parseDouble(content.substring(hyracksTimeIx + 14, endHyracksTimeIx));

        int totalTimeSIx = content.indexOf("Duration", endHyracksTimeIx);
        if (totalTimeSIx < 0) {
            return new double[] { hTime, -1 };
        }
        int totalTimeEIx = content.indexOf("\n", totalTimeSIx);
        double tTime = Double.parseDouble(content.substring(totalTimeSIx + 10, totalTimeEIx));

        return new double[] { hTime, tTime };
    }

}