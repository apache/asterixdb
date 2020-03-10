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

import org.apache.asterix.clienthelper.Args;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FilenameUtils;
import org.apache.hyracks.util.file.FileUtil;
import org.mindrot.jbcrypt.BCrypt;

public class GenerateCredentialCommand extends ClientCommand {

    private static String CREDENTIAL_FILE_NAME = "passwd";

    public GenerateCredentialCommand(Args args) {
        super(args);
    }

    @Override
    public int execute() throws IOException {
        String salt = BCrypt.gensalt(12);
        String username = args.getUsername();
        String password = args.getPassword();
        String passwordHash = BCrypt.hashpw(password, salt);
        File passwd =
                new File(FilenameUtils.normalize(FileUtil.joinPath(args.getCredentialPath(), CREDENTIAL_FILE_NAME)));
        if (!passwd.exists()) {
            passwd.createNewFile();
        }
        try (CSVPrinter p =
                new CSVPrinter(new FileWriter(passwd.getAbsolutePath(), true), CSVFormat.DEFAULT.withDelimiter(':'))) {
            p.printRecord(username, passwordHash);
        }
        return 0;
    }
}
