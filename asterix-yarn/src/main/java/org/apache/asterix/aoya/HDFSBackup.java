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
package org.apache.asterix.aoya;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class HDFSBackup {
    Configuration conf = new YarnConfiguration();
    private static final Log LOG = LogFactory.getLog(AsterixApplicationMaster.class);
    boolean restore = false;
    boolean backup = false;

    public static void main(String[] args) throws ParseException, IllegalArgumentException, IOException {

        HDFSBackup back = new HDFSBackup();
        Map<String, String> envs = System.getenv();
        if (envs.containsKey("HADOOP_CONF_DIR")) {
            File hadoopConfDir = new File(envs.get("HADOOP_CONF_DIR"));
            if (hadoopConfDir.isDirectory()) {
                for (File config : hadoopConfDir.listFiles()) {
                    if (config.getName().matches("^.*(xml)$")) {
                        back.conf.addResource(new Path(config.getAbsolutePath()));
                    }
                }
            }
        }
        Options opts = new Options();
        opts.addOption("restore", false, "");
        opts.addOption("backup", false, "");
        CommandLine cliParser = new GnuParser().parse(opts, args);
        if (cliParser.hasOption("restore")) {
            back.restore = true;
        }
        if (cliParser.hasOption("backup")) {
            back.backup = true;
        }
        @SuppressWarnings("unchecked")
        List<String> pairs = cliParser.getArgList();

        List<Path[]> sources = new ArrayList<Path[]>(10);
        for (String p : pairs) {
            String[] s = p.split(",");
            sources.add(new Path[] { new Path(s[0]), new Path(s[1]) });
        }

        try {
            if (back.backup) {
                back.performBackup(sources);
            }
            if (back.restore) {
                back.performRestore(sources);
            }
        } catch (IOException e) {
            LOG.fatal("Backup/restoration unsuccessful: " + e.getMessage());
            throw e;
        }
    }

    private void performBackup(List<Path[]> paths) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        for (Path[] p : paths) {
            LOG.info("Backing up " + p[0] + " to " + p[1] + ".");
            fs.copyFromLocalFile(p[0], p[1]);
        }
    }

    private void performRestore(List<Path[]> paths) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        for (Path[] p : paths) {
            LOG.info("Restoring " + p[0] + " to " + p[1] + ".");
            File f = new File(p[1].toString() + File.separator + p[0].getName());
            LOG.info(f.getAbsolutePath());
            if (f.exists()) {
                FileUtils.deleteDirectory(f);
            }
            LOG.info(f.exists());
            fs.copyToLocalFile(false, p[0], p[1], true);
        }
    }
}
