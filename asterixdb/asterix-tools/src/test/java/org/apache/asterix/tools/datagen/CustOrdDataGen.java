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
package org.apache.asterix.tools.datagen;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class CustOrdDataGen {

    // for customers
    private static final String FIRST_NAMES_FILE_NAME = "/opt/us_census_names/dist.all.first.cleaned";
    private static final String LAST_NAMES_FILE_NAME = "/opt/us_census_names/dist.all.last.cleaned";
    private static final int MIN_AGE = 10;
    private static final int MAX_AGE = 90;

    private static final String[] STREETS = { "Main St.", "Oak St.", "7th St.", "Washington St.", "Cedar St.",
            "Lake St.", "Hill St.", "Park St.", "View St." };
    private static final int MIN_STREET_NUM = 1;
    private static final int MAX_STREET_NUM = 10000;
    private static final String[] CITIES =
            { "Seattle", "San Jose", "Mountain View", "Los Angeles", "Sunnyvale", "Portland" };

    private static final int MIN_INTERESTS = 0;
    private static final int MAX_INTERESTS = 5;
    private String[] INTERESTS = { "Bass", "Music", "Databases", "Fishing", "Tennis", "Squash", "Computers", "Books",
            "Movies", "Cigars", "Wine", "Running", "Walking", "Skiing", "Basketball", "Video Games", "Cooking",
            "Coffee", "Base Jumping", "Puzzles", "Chess", "Programming", "Reddit", "Soccer", "Hockey", "Money",
            "Dancing", "Brewing", "Gardening", "Hacking", "Reading" };

    private static final int MIN_CHILD_AGE = 0;

    private static final int MIN_CHILDREN = 0;
    private static final int MAX_CHILDREN = 5;

    // for orders
    private static final int MIN_ORDERS_PER_CUST = 1;
    private static final int MAX_ORDERS_PER_CUST = 10;

    private String[] CLERKS = { "Kathrin", "Katherine", "Cathryn", "Catherine", "Cat", "Kathryne", "Cathrin" };
    private String[] ORDER_PRIORITIES = { "LOW", "MEDIUM", "HIGH", "PREMIUM" };
    private String[] ORDER_STATUSES = { "ORDER_PLACED", "PAYMENT_RECEIVED", "ORDER_SHIPPED", "ORDER_DELIVERED" };

    private String[] firstNames =
            { "Joe", "John", "Jill", "Gill", "Bill", "William", "Kathy", "Cathey", "Jane", "Albert" };
    private String[] lastNames =
            { "Doe", "Smith", "Li", "Singh", "Williams", "Davis", "Brown", "Wilson", "Moore", "Thomas" };

    private static final String[] UNDECLARED_FIELD_NAMES =
            { "param1", "param2", "param3", "param4", "param5", "param6", "param7", "param8", "param9", "param10" };

    private int currentCID = 0;
    private int currentOID = 0;

    private Random rndValue = new Random(50);

    private Random rndChildAgeField = new Random(50);
    private Random rndCustAgeField = new Random(50);
    private Random rndCustAddressField = new Random(50);

    private Random[] rndUndeclaredOrderFields;

    private class Child {
        public String name;
        public int age;

        public void generateFieldValues(String lastName, int maxChildAge) {
            // name
            int firstNameIx = Math.abs(rndValue.nextInt()) % firstNames.length;
            name = firstNames[firstNameIx] + " " + lastName;

            // age
            age = -1;
            if (rndChildAgeField.nextBoolean()) {
                if (maxChildAge >= 0) {
                    if (maxChildAge == MIN_CHILD_AGE) {
                        age = maxChildAge;
                    } else {
                        age = Math.abs((rndValue.nextInt()) % (maxChildAge - MIN_CHILD_AGE)) + MIN_AGE;
                    }
                }
            }
        }

        public String getJSON() {
            StringBuilder jsonString = new StringBuilder();

            jsonString.append("{ "); // start child

            // name
            jsonString.append(" \"name\": ");
            jsonString.append("\"" + name + "\"");

            // age
            if (age >= 0) {
                jsonString.append(", ");
                jsonString.append(" \"age\": ");
                jsonString.append(age);
            }

            jsonString.append(" }"); // end child

            return jsonString.toString();
        }

    }

    private class Customer {

        private int cid;
        private String name;
        private int age;

        private String streetName;
        private int streetNumber;
        private String city;

        private int[] custInterests;
        private Child[] custChildren;

        public void generateFieldValues() {
            cid = currentCID++;

            int firstNameIx = Math.abs(rndValue.nextInt()) % firstNames.length;
            int lastNameIx = Math.abs(rndValue.nextInt()) % lastNames.length;
            name = firstNames[firstNameIx] + " " + lastNames[lastNameIx];

            if (rndCustAgeField.nextBoolean()) {
                age = Math.abs((rndValue.nextInt()) % (MAX_AGE - MIN_AGE)) + MIN_AGE;
            } else {
                age = -1;
            }

            if (rndCustAddressField.nextBoolean()) {
                streetNumber = Math.abs(rndValue.nextInt()) % (MAX_STREET_NUM - MIN_STREET_NUM) + MIN_STREET_NUM;

                int streetIx = Math.abs(rndValue.nextInt()) % STREETS.length;
                streetName = STREETS[streetIx];

                int cityIx = Math.abs(rndValue.nextInt()) % CITIES.length;
                city = CITIES[cityIx];
            } else {
                streetNumber = -1;
                streetName = null;
                city = null;
            }

            int numInterests = Math.abs((rndValue.nextInt()) % (MAX_INTERESTS - MIN_INTERESTS)) + MIN_INTERESTS;
            custInterests = new int[numInterests];
            for (int i = 0; i < numInterests; i++) {
                custInterests[i] = Math.abs(rndValue.nextInt()) % INTERESTS.length;
            }

            int numChildren = Math.abs((rndValue.nextInt()) % (MAX_CHILDREN - MIN_CHILDREN)) + MIN_CHILDREN;
            custChildren = new Child[numChildren];
            for (int i = 0; i < numChildren; i++) {
                Child c = new Child();
                int maxChildAge = age <= 0 ? 50 : (age - 20);
                c.generateFieldValues(lastNames[lastNameIx], maxChildAge);
                custChildren[i] = c;
            }
        }

        public String getJSON() {
            StringBuilder jsonString = new StringBuilder();

            jsonString.append("{ "); // start customer

            // customer id
            jsonString.append(" \"cid\": ");
            jsonString.append(cid);
            jsonString.append(", ");

            // name
            jsonString.append(" \"name\": ");
            jsonString.append("\"" + name + "\"");

            // age
            if (age >= 0) {
                jsonString.append(", ");
                jsonString.append(" \"age\": ");
                jsonString.append(age);
            }

            // nested address
            if (streetNumber >= 0) {
                jsonString.append(", ");
                jsonString.append(" \"address\": ");
                jsonString.append("{ "); // start address

                // number
                jsonString.append(" \"number\": ");
                jsonString.append(streetNumber);
                jsonString.append(", ");

                // street
                jsonString.append(" \"street\": ");
                jsonString.append("\"" + streetName + "\"");
                jsonString.append(", ");

                // city
                jsonString.append(" \"city\": ");
                jsonString.append("\"" + city + "\"");

                jsonString.append(" }"); // end address
            }

            jsonString.append(", ");

            // interests
            jsonString.append(" \"interests\": ");
            jsonString.append("{{ "); // start interests
            for (int i = 0; i < custInterests.length; i++) {
                jsonString.append("\"" + INTERESTS[custInterests[i]] + "\"");
                if (i != custInterests.length - 1) {
                    jsonString.append(", ");
                }
            }
            jsonString.append(" }}"); // end interests
            jsonString.append(", ");

            // children
            jsonString.append(" \"children\": ");
            jsonString.append("[ "); // start children
            for (int i = 0; i < custChildren.length; i++) {
                String jsonChild = custChildren[i].getJSON();
                jsonString.append(jsonChild);
                if (i != custChildren.length - 1) {
                    jsonString.append(", ");
                }
            }
            jsonString.append(" ]"); // end children

            jsonString.append(" }"); // end customer

            return jsonString.toString();
        }
    }

    private class Order {
        public int oid;
        public int cid;
        public String orderStatus;
        public String orderPriority;
        public String clerk;
        public String total;

        private int[] undeclaredFields;
        private BitSet nullMap;

        public void generateFieldValues(Customer cust) {
            cid = cust.cid;
            oid = currentOID++;
            float t = Math.abs(rndValue.nextFloat()) * 100;
            total = t + "f";

            int orderStatusIx = Math.abs(rndValue.nextInt()) % ORDER_STATUSES.length;
            orderStatus = ORDER_STATUSES[orderStatusIx];

            int orderPriorityIx = Math.abs(rndValue.nextInt()) % ORDER_PRIORITIES.length;
            orderPriority = ORDER_PRIORITIES[orderPriorityIx];

            int clerkIx = Math.abs(rndValue.nextInt()) % CLERKS.length;
            clerk = CLERKS[clerkIx];

            int m = rndUndeclaredOrderFields.length;
            undeclaredFields = new int[m];
            nullMap = new BitSet(m);
            for (int i = 0; i < m; i++) {
                if (rndUndeclaredOrderFields[i].nextBoolean()) {
                    undeclaredFields[i] = rndValue.nextInt();
                } else {
                    nullMap.set(i);
                }
            }
        }

        public String getJSON() {
            StringBuilder jsonString = new StringBuilder();

            jsonString.append("{ "); // start order

            // oid
            jsonString.append(" \"oid\": ");
            jsonString.append(oid);
            jsonString.append(", ");

            // cid
            jsonString.append(" \"cid\": ");
            jsonString.append(cid);
            jsonString.append(", ");

            // orderStatus
            jsonString.append(" \"orderstatus\": ");
            jsonString.append("\"" + orderStatus + "\"");
            jsonString.append(", ");

            // orderPriority
            jsonString.append(" \"orderpriority\": ");
            jsonString.append("\"" + orderPriority + "\"");
            jsonString.append(", ");

            // clerk
            jsonString.append(" \"clerk\": ");
            jsonString.append("\"" + clerk + "\"");
            jsonString.append(", ");

            // / cid
            jsonString.append(" \"total\": ");
            jsonString.append(total);

            for (int i = 0; i < undeclaredFields.length; i++) {
                if (!nullMap.get(i)) {
                    jsonString.append(", ");
                    jsonString.append(" \"" + UNDECLARED_FIELD_NAMES[i] + "\": ");
                    jsonString.append(undeclaredFields[i]);
                }
            }

            jsonString.append(" }"); // end order
            return jsonString.toString();
        }
    }

    public void init() {
        try {
            List<String> tmpFirstNames = new ArrayList<String>();

            FileInputStream fstream = new FileInputStream(FIRST_NAMES_FILE_NAME);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            while ((strLine = br.readLine()) != null) {
                String firstLetter = strLine.substring(0, 1);
                String remainder = strLine.substring(1);
                String capitalized = firstLetter.toUpperCase() + remainder.toLowerCase();
                tmpFirstNames.add(capitalized);
            }
            in.close();
            firstNames = tmpFirstNames.toArray(firstNames);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            List<String> tmpLastNames = new ArrayList<String>();

            FileInputStream fstream = new FileInputStream(LAST_NAMES_FILE_NAME);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            while ((strLine = br.readLine()) != null) {
                String firstLetter = strLine.substring(0, 1);
                String remainder = strLine.substring(1);
                String capitalized = firstLetter.toUpperCase() + remainder.toLowerCase();
                tmpLastNames.add(capitalized);
            }
            in.close();
            lastNames = tmpLastNames.toArray(firstNames);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        rndUndeclaredOrderFields = new Random[UNDECLARED_FIELD_NAMES.length];
        for (int i = 0; i < rndUndeclaredOrderFields.length; i++) {
            rndUndeclaredOrderFields[i] = new Random(50);
        }
    }

    public void writeOrdersList(List<Order> ordersList, FileWriter ordersFile) throws IOException {
        while (!ordersList.isEmpty()) {
            int ix = Math.abs(rndValue.nextInt()) % ordersList.size();
            ordersFile.write(ordersList.get(ix).getJSON() + "\n");
            ordersList.remove(ix);
        }
    }

    public void writeCustomerList(List<Customer> customerList, Order[] ordersBatch, List<Order> ordersList,
            FileWriter customersFile, FileWriter ordersFile) throws IOException {
        while (!customerList.isEmpty()) {
            int ix = Math.abs(rndValue.nextInt()) % customerList.size();
            customersFile.write(customerList.get(ix).getJSON() + "\n");

            // generate orders
            int numOrders =
                    Math.abs(rndValue.nextInt()) % (MAX_ORDERS_PER_CUST - MIN_ORDERS_PER_CUST) + MIN_ORDERS_PER_CUST;
            for (int i = 0; i < numOrders; i++) {
                ordersBatch[i].generateFieldValues(customerList.get(ix));
                ordersList.add(ordersBatch[i]);
            }
            writeOrdersList(ordersList, ordersFile);

            customerList.remove(ix);
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("MUST PROVIDE 2 PARAMS, 1. output dir name and 2. number of records to generate.");
            System.exit(1);
        }

        String outputFile = args[0];
        int numRecords = Integer.parseInt(args[1]);

        FileWriter customersFile = new FileWriter(outputFile + File.separator + "customer.adm");
        FileWriter ordersFile = new FileWriter(outputFile + File.separator + "orders.adm");

        CustOrdDataGen dataGen = new CustOrdDataGen();
        dataGen.init();

        int batchSize = 1000;
        Customer[] customerBatch = new Customer[batchSize];
        for (int i = 0; i < batchSize; i++) {
            customerBatch[i] = dataGen.new Customer();
        }

        Order[] ordersBatch = new Order[MAX_ORDERS_PER_CUST];
        for (int i = 0; i < MAX_ORDERS_PER_CUST; i++) {
            ordersBatch[i] = dataGen.new Order();
        }

        List<Customer> customerList = new LinkedList<Customer>();
        List<Order> ordersList = new LinkedList<Order>();
        int custIx = 0;
        for (int i = 0; i < numRecords; i++) {

            customerBatch[custIx].generateFieldValues();
            customerList.add(customerBatch[custIx]);
            custIx++;

            if (customerList.size() >= batchSize) {
                dataGen.writeCustomerList(customerList, ordersBatch, ordersList, customersFile, ordersFile);
                custIx = 0;
            }
        }
        dataGen.writeCustomerList(customerList, ordersBatch, ordersList, customersFile, ordersFile);
        customersFile.flush();
        customersFile.close();

        ordersFile.flush();
        ordersFile.close();
    }
}
