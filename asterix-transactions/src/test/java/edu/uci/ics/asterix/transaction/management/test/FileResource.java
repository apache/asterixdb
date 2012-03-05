/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.asterix.transaction.management.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import edu.uci.ics.asterix.transaction.management.logging.IResource;
import edu.uci.ics.asterix.transaction.management.service.logging.FileUtil;
import edu.uci.ics.asterix.transaction.management.service.logging.ILogger;

class FileResource implements IResource {

    private byte[] resourceId = new byte[] { 1 };
    private ILogger logger;

    private File file;
    private int memCounter = 0;
    private int diskCounter = 0;

    public int getMemoryCounter() {
        return memCounter;
    }

    public int getDiskCounter() {
        return diskCounter;
    }

    public static enum CounterOperation {
        INCREMENT,
        DECREMENT,
    };

    public FileResource(String fileDir, String fileName) throws IOException {
        File dirFile = new File(fileDir);
        if (!dirFile.exists()) {
            FileUtil.createNewDirectory(fileDir);
        }
        file = new File(fileDir + "/" + fileName);
        if (!file.exists()) {
            FileUtil.createFileIfNotExists(file.getAbsolutePath());
            BufferedWriter writer = new BufferedWriter(new FileWriter(file));
            writer.write("0");
            writer.flush();
        } else {
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String content = bufferedReader.readLine();
            diskCounter = Integer.parseInt(content);
        }
        logger = new FileLogger(this);
    }

    public synchronized void increment() {
        memCounter++;
    }

    public synchronized void decrement() {
        memCounter--;
    }

    public synchronized void setValue(int value) {
        memCounter = value;
    }

    public synchronized void sync() throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        writer.write("" + memCounter);
        writer.flush();
    }

    public synchronized boolean checkIfValueInSync(int expectedValue) throws IOException {
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String content = bufferedReader.readLine();
        return content.equals("" + expectedValue);
    }

    @Override
    public byte[] getId() {
        return resourceId;
    }

    @Override
    public ILogger getLogger() {
        return logger;
    }
}
