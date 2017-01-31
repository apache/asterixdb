/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterixdb.aws;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * This class is the binary that automatically generates :
 * 1. an Ansible inventory file for the AWS cluster;
 * 2. an AsterixDB configuration file for the AWS cluster
 * from a JSON description file returned by the Ansible AWS startup script.
 */
public class ConfigGenerator {

    private ConfigGenerator() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("The usage of ConfigGenerator: ");
            System.err.println("<input node json file> <output inventory file> <output config file>");
            System.exit(0);
        }
        String source = args[0];
        String inventory = args[1];
        String config = args[2];

        // Read Json file data to String
        byte[] jsonData = Files.readAllBytes(Paths.get(source));

        // Get a list of cluster nodes
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode root = (ObjectNode) objectMapper.readTree(jsonData);
        ArrayNode nodes = (ArrayNode) root.get("tagged_instances");
        Iterator<JsonNode> nodeIterator = nodes.iterator();
        List<AwsNode> cluster = new ArrayList<>();
        while (nodeIterator.hasNext()) {
            ObjectNode node = (ObjectNode) nodeIterator.next();
            cluster.add(new AwsNode(node));
        }

        if (cluster.isEmpty()) {
            return;
        }

        // Generates inventory file.
        generateInventoryFile(cluster, inventory);

        // Generates asterixdb config
        generateConfig(cluster, config);
    }

    private static void generateInventoryFile(List<AwsNode> cluster, String inventoryPath) throws IOException {
        Iterator<AwsNode> nodeIterator = cluster.iterator();
        try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(inventoryPath)))) {
            // Prints the cc section
            writer.println("[cc]");
            String masterIp = nodeIterator.next().getDnsName();
            writer.println(masterIp);
            writer.println();

            // Prints the nc section
            writer.println("[ncs]");
            writer.println(masterIp); // There is a NC that co-locates with CC.
            while (nodeIterator.hasNext()) {
                writer.println(nodeIterator.next().getDnsName());
            }
            writer.println();

            // Prints the user
            writer.println("[all:vars]");
            writer.println("ansible_ssh_user=ec2-user");
        }
    }

    private static void generateConfig(List<AwsNode> cluster, String configPath) throws IOException {
        Iterator<AwsNode> nodeIterator = cluster.iterator();
        try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(configPath)))) {
            // Prints the cc section
            writer.println("[cc]");
            String masterIp = nodeIterator.next().getPrivateIp();
            writer.println("cluster.address=" + masterIp);
            writer.println();

            // Prints the nc section
            writer.println("[nc/1]");
            writer.println("address=" + masterIp); // There is a NC that co-locates with CC.
            int ncCounter = 2;
            while (nodeIterator.hasNext()) {
                writer.println("[nc/" + ncCounter++ + "]");
                writer.println("address=" + nodeIterator.next().getPrivateIp());
                writer.println();
            }

            // Prints the nc parameter section.
            writer.println("[nc]");
            String rootDirectory = "/home/ec2-user/";
            writer.println("txnlogdir=" + rootDirectory + "txnlog");

            // TODO(yingyi): figure out how to get device mapping for SSD-based instances.
            writer.println("iodevices=" + rootDirectory + "iodevice");
            writer.println("command=asterixnc");
            writer.println();
        }
    }
}
