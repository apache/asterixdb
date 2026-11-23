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
package org.apache.asterix.clienthelper.commands;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.SecureRandom;

import org.apache.asterix.clienthelper.Args;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FilenameUtils;
import org.apache.hyracks.util.file.FileUtil;
import org.mindrot.jbcrypt.BCrypt;

public class GenerateDefaultCredentialCommand extends ClientCommand {

    private static final String CREDENTIAL_FILE_NAME = "passwd";
    private static final String DEFAULT_USERNAME = "admin";
    private static final String ALPHANUMERIC = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final int PASSWORD_LENGTH = 24;

    public GenerateDefaultCredentialCommand(Args args) {
        super(args);
    }

    @Override
    public int execute() throws IOException {
        String password = generateRandomPassword();
        String salt = BCrypt.gensalt(12);
        String passwordHash = BCrypt.hashpw(password, salt);
        File passwd =
                new File(FilenameUtils.normalize(FileUtil.joinPath(args.getCredentialPath(), CREDENTIAL_FILE_NAME)));
        passwd.getParentFile().mkdirs();
        try (CSVPrinter p =
                new CSVPrinter(new FileWriter(passwd.getAbsolutePath(), false), CSVFormat.DEFAULT.withDelimiter(':'))) {
            p.printRecord(DEFAULT_USERNAME, passwordHash);
        }
        System.out.println(password);
        return 0;
    }

    private static String generateRandomPassword() {
        SecureRandom random = new SecureRandom();
        StringBuilder sb = new StringBuilder(PASSWORD_LENGTH);
        for (int i = 0; i < PASSWORD_LENGTH; i++) {
            sb.append(ALPHANUMERIC.charAt(random.nextInt(ALPHANUMERIC.length())));
        }
        return sb.toString();
    }
}
