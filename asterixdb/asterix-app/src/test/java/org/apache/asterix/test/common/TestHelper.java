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
package org.apache.asterix.test.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.asterix.common.configuration.AsterixConfiguration;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

public final class TestHelper {

    private static final String TEST_DIR_BASE_PATH = System.getProperty("user.dir") + File.separator + "target";
    private static final String[] TEST_DIRS = new String[] { "txnLogDir", "IODevice", "spill_area", "config" };

    public static boolean isInPrefixList(List<String> prefixList, String s) {
        for (String s2 : prefixList) {
            if (s.startsWith(s2)) {
                return true;
            }
        }
        return false;
    }

    public static String joinPath(String... pathElements) {
        return StringUtils.join(pathElements, File.separatorChar);
    }

    public static void unzip(String sourceFile, String outputDir) throws IOException {
        if (System.getProperty("os.name").toLowerCase().startsWith("win")) {
            try (ZipFile zipFile = new ZipFile(sourceFile)) {
                Enumeration<? extends ZipEntry> entries = zipFile.entries();
                while (entries.hasMoreElements()) {
                    ZipEntry entry = entries.nextElement();
                    File entryDestination = new File(outputDir, entry.getName());
                    if (!entry.isDirectory()) {
                        entryDestination.getParentFile().mkdirs();
                        try (InputStream in = zipFile.getInputStream(entry);
                                OutputStream out = new FileOutputStream(entryDestination)) {
                            IOUtils.copy(in, out);
                        }
                    }
                }
            }
        } else {
            Process process = new ProcessBuilder("unzip", "-d", outputDir, sourceFile).start();
            try {
                process.waitFor();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
        }
    }

    public static AsterixConfiguration getConfigurations(String fileName)
            throws IOException, JAXBException, AsterixException {
        try (InputStream is = TestHelper.class.getClassLoader().getResourceAsStream(fileName)) {
            if (is != null) {
                JAXBContext ctx = JAXBContext.newInstance(AsterixConfiguration.class);
                Unmarshaller unmarshaller = ctx.createUnmarshaller();
                return (AsterixConfiguration) unmarshaller.unmarshal(is);
            } else {
                throw new AsterixException("Could not find configuration file " + fileName);
            }
        }
    }

    public static void writeConfigurations(AsterixConfiguration ac, String fileName)
            throws FileNotFoundException, IOException, JAXBException {
        File configFile = new File(fileName);
        if (!configFile.exists()) {
            configFile.getParentFile().mkdirs();
            configFile.createNewFile();
        } else {
            configFile.delete();
        }
        try (FileOutputStream os = new FileOutputStream(fileName)) {
            JAXBContext ctx = JAXBContext.newInstance(AsterixConfiguration.class);
            Marshaller marshaller = ctx.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            marshaller.marshal(ac, os);
        }
    }

    public static void deleteExistingInstanceFiles() {
        for (String dirName : TEST_DIRS) {
            File f = new File(joinPath(TEST_DIR_BASE_PATH, dirName));
            if (FileUtils.deleteQuietly(f)) {
                System.out.println("Dir " + f.getName() + " deleted");
            }
        }
    }
}