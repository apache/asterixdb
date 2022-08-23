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

package org.apache.hyracks.algebricks.core.rewriter.base;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CardHints {

    private static final double MAX_CARD = 1.0e200;

    private List<List<String>> listRefNames;
    private List<Double> cards;
    private List<Double> sels;
    private List<Double> sizes;

    public CardHints() {
        listRefNames = new ArrayList<>();
        cards = new ArrayList<>();
        sels = new ArrayList<>();
        sizes = new ArrayList<>();
    }

    public void setCardInfo(List<List<String>> names, List<Double> c, List<Double> s, List<Double> si) {
        listRefNames = names;
        cards = c;
        sels = s;
        sizes = si;
    }

    public List<List<String>> getListRefNames() {
        return listRefNames;
    }

    public List<Double> getCards() {
        return cards;
    }

    public List<Double> getSels() {
        return sels;
    }

    public List<Double> getSizes() {
        return sizes;
    }

    public static List<String> isolateVariables(List<String> varsLogical) { // comes from the FromList
        Pattern var = Pattern.compile("\\D\\w*");

        List<String> vars = new ArrayList<>();

        for (String vl : varsLogical) {
            Matcher mat = var.matcher(vl.toString());
            if (mat.find()) {
                vars.add(mat.group());
            }
        }
        Collections.sort(vars);
        return vars;
    }

    public double findCardinality(List<String> vars) {
        double card = MAX_CARD; // default value is set to high if no hint information is available
        int i = 0;
        for (List<String> refnames : this.getListRefNames()) {
            if (refnames.equals(vars))
            //return this.getCards().get(i) * this.getSels().get(i);
            {
                return this.getCards().get(i); // we want the original cardinality without any selections
            }
            i++;
        }
        return card;
    }

    public double findSize(List<String> vars) {
        int i = 0;
        for (List<String> refnames : this.getListRefNames()) {
            if (refnames.equals(vars)) {
                return this.getSizes().get(i);
            }
            i++;
        }
        return MAX_CARD;
    }

    // This routine should not be invoked anymore as we are only providing base hints.
    // But in the event a user does specify mutiple table cards, then this routine will be invoked. Check!
    public double findCardinality(List<String> varsL, List<String> varsR) {
        varsL.addAll(varsR);
        Collections.sort(varsL);
        return this.findCardinality(varsL);
    }

    public static CardHints getCardHintsInfo(String hintParams) {
        Pattern var = Pattern.compile("[a-zA-Z]\\w*"); // any word character [a-zA-Z_0-9]

        Pattern number = Pattern.compile("\\d+\\.\\d+");

        Pattern singleHintPattern =
                Pattern.compile("\\(\\s*\\w+[\\s+\\w+\\s*]*\\s+\\d+\\.\\d+\\s+\\d+\\.\\d+\\s+\\d+\\.\\d+\\s*\\)");

        // The above pattern is (id1 id2 ... idn first number second number third number)
        // (o 400.0 0.5) or (o l s 4000.0 0.025) etc
        // Note that the 2nd number which is selectivity is not optional. It can always be set to 1.0.
        List<List<String>> listRefNames = new ArrayList<>();
        List<Double> cards = new ArrayList<>();
        List<Double> sels = new ArrayList<>();
        List<Double> sizes = new ArrayList<>();
        CardHints cardinalityHints = new CardHints();
        if (hintParams != null) {
            Matcher matCHP = singleHintPattern.matcher(hintParams);

            while (matCHP.find()) {
                List<String> refNames = new ArrayList<>();
                Double selectivity = 1.0;
                Double cardinality = -1.0;
                Double size = 1.0;
                String matchedGroup = matCHP.group();
                Matcher matVar = var.matcher(matchedGroup);
                Matcher numMat = number.matcher(matchedGroup);

                while (matVar.find()) {
                    refNames.add(matVar.group()); // find all the ids first
                }

                int i = 0;
                while (numMat.find()) {
                    i++;
                    switch (i) {
                        case 1:
                            cardinality = Double.parseDouble(numMat.group());
                            break;
                        case 2:
                            selectivity = Double.parseDouble(numMat.group());
                        case 3:
                            size = Double.parseDouble(numMat.group());
                        default: // add error checking here.
                            ;
                    }
                }
                Collections.sort(refNames);
                listRefNames.add(refNames);
                cards.add(cardinality);
                sels.add(selectivity);
                sizes.add(size);

                cardinalityHints.setCardInfo(listRefNames, cards, sels, sizes);
            }

            return cardinalityHints;
        }
        return null; // keeps compiler happy.
    }
}