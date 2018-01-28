/**
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

package org.apache.asterix.fuzzyjoin.tests.dataset;

import java.io.File;
import java.util.NoSuchElementException;

public abstract class AbstractDataset {
    public static enum Directory {
        RAW_R,
        RAW_S,
        RECORDPAIRS,
        RECORDS_R,
        RECORDS_S,
        RECORDSBULK_R,
        RECORDSBULK_S,
        RIDPAIRS,
        SSJOININ,
        SSJOINOUT,
        TOKENS,
        TOKENS_R,
        TOKENS_R_AQL,
    }

    public static enum Relation {
        R,
        S,
    }

    public static final String FILE_PART = "part-";
    public static final String FILE_PART0 = FILE_PART + "00000";
    public static final String FILE_EXPECTED = "expected.txt";
    public static final String AQL = "aql";

    public static final String PATH_RAW = "raw";
    public static final String PATH_RECORDPAIRS = "recordpairs";
    public static final String PATH_RECORDS = "records";
    public static final String PATH_RECORDSBULK = "recordsbulk";
    public static final String PATH_RIDPAIRS = "ridpairs";
    public static final String PATH_SSJOININ = "ssjoin.in";
    public static final String PATH_SSJOINOUT = "ssjoin.out";
    public static final String PATH_TOKENS = "tokens";

    public static final String DIRECTORY_ID_FORMAT = "%03d";

    public void createDirecotries(String[] paths) {
        createDirecotries(paths, 0);
    }

    public void createDirecotries(String[] paths, int crtCopy) {
        (new File(paths[0] + getPathDirecotry(Directory.SSJOINOUT, 0))).mkdir();
        (new File(paths[0] + getPathDirecotry(Directory.RECORDSBULK_R, crtCopy))).mkdir();
        (new File(paths[0] + getPathDirecotry(Directory.RECORDSBULK_S, crtCopy))).mkdir();
        (new File(paths[0] + getPathDirecotry(Directory.RECORDS_R, crtCopy))).mkdir();
        (new File(paths[0] + getPathDirecotry(Directory.RECORDS_S, crtCopy))).mkdir();
        (new File(paths[0] + getPathDirecotry(Directory.TOKENS, crtCopy))).mkdir();
        (new File(paths[0] + getPathDirecotry(Directory.TOKENS_R, crtCopy))).mkdir();
        (new File(paths[0] + getPathDirecotry(Directory.TOKENS_R_AQL, crtCopy))).mkdir();
    }

    public abstract String getName();

    public abstract int getNoRecords();

    public abstract String getPath();

    public String getPathDirecotry(Directory directory, int crtCopy) {
        return getPathDirectory(getPath(), directory, crtCopy);
    }

    private String getPathDirectory(Directory directory, int crtCopy, boolean expected) {
        return getPathDirectory(getName() + (expected ? ".expected" : ""), directory, crtCopy);
    }

    public String getPathDirectory(String path, Directory directory, int crtCopy) {
        path += '/';
        switch (directory) {
            case SSJOININ:
                path += AbstractDataset.PATH_SSJOININ;
                break;
            case SSJOINOUT:
                path += AbstractDataset.PATH_SSJOINOUT;
                break;
            case RAW_R:
                path += AbstractDataset.PATH_RAW + "." + getSuffix(Relation.R);
                break;
            case RAW_S:
                path += AbstractDataset.PATH_RAW + "." + getSuffix(Relation.S);
                break;
            case RECORDSBULK_R:
                path += AbstractDataset.PATH_RECORDSBULK + "." + getSuffix(Relation.R);
                break;
            case RECORDSBULK_S:
                path += AbstractDataset.PATH_RECORDSBULK + "." + getSuffix(Relation.S);
                break;
            case RECORDS_R:
                path += AbstractDataset.PATH_RECORDS + "." + getSuffix(Relation.R);
                break;
            case RECORDS_S:
                path += AbstractDataset.PATH_RECORDS + "." + getSuffix(Relation.S);
                break;
            case TOKENS:
                path += AbstractDataset.PATH_TOKENS;
                break;
            case TOKENS_R:
                path += AbstractDataset.PATH_TOKENS + "." + getSuffix(Relation.R);
                break;
            case TOKENS_R_AQL:
                path += AbstractDataset.PATH_TOKENS + "." + getSuffix(Relation.R) + "." + AQL;
                break;
            case RIDPAIRS:
                path += AbstractDataset.PATH_RIDPAIRS;
                break;
            case RECORDPAIRS:
                path += AbstractDataset.PATH_RECORDPAIRS;
                break;
            default:
                throw new NoSuchElementException();
        }
        return path + "-" + String.format(DIRECTORY_ID_FORMAT, crtCopy);
    }

    public String getPathExpected(Directory directory) {
        return getPathDirectory(directory, 0, true) + '/' + FILE_EXPECTED;
    }

    public String getPathPart(Directory directory, int crtCopy) {
        return getPathDirecotry(directory, crtCopy) + '/' + FILE_PART;
    }

    public String getPathPart0(Directory directory) {
        return getPathDirectory(directory, 0, false) + '/' + FILE_PART0;
    }

    public String getPathPart0(Directory directory, boolean expected) {
        return getPathDirectory(directory, 0, expected) + '/' + (expected ? FILE_EXPECTED : FILE_PART0);
    }

    public abstract String getSuffix(Relation relation);

    public abstract float getThreshold();
}
