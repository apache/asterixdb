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
package org.apache.asterix.test.podman;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.testcontainers.images.builder.ImageFromDockerfile;

/**
 * Builds the image defined by the module's Containerfile from the server assembly, which asterix-server must have
 * built already.
 */
public final class ServerImage {
    private static final Path IMAGE_DIR = Path.of("src/main/resources");
    private static final Path SERVER_DIR = Path.of("../asterix-server/target");

    private ServerImage() {
    }

    /**
     * @param name
     *            the name and tag to give the image, which is deleted when the JVM exits
     * @return the name of the built image
     */
    public static String build(String name) throws Exception {
        Path assembly = serverAssembly();
        return new ImageFromDockerfile(name, true).withFileFromPath("Dockerfile", IMAGE_DIR.resolve("Containerfile"))
                .withFileFromPath("target/entrypoint.sh", IMAGE_DIR.resolve("entrypoint.sh"))
                .withFileFromPath("target/" + assembly.getFileName(), assembly).get();
    }

    private static Path serverAssembly() throws IOException {
        List<Path> assemblies = new ArrayList<>();
        try (DirectoryStream<Path> stream =
                Files.newDirectoryStream(SERVER_DIR, "asterix-server-*-binary-assembly.tar.bz2")) {
            stream.forEach(assemblies::add);
        }
        assertEquals("build asterix-server first; found " + assemblies, 1, assemblies.size());
        return assemblies.get(0);
    }
}
