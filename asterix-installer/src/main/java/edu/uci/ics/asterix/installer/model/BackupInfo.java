/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.asterix.installer.model;

import java.io.Serializable;
import java.util.Date;

import edu.uci.ics.asterix.installer.schema.conf.Backup;
import edu.uci.ics.asterix.installer.schema.conf.Hdfs;

public class BackupInfo implements Serializable {

    public static enum BackupType {
        LOCAL,
        HDFS
    };

    private final int id;
    private final Date date;
    private final Backup backupConf;

    public BackupInfo(int id, Date date, Backup backupConf) {
        this.id = id;
        this.date = date;
        this.backupConf = backupConf;
    }

    public int getId() {
        return id;
    }

    public Date getDate() {
        return date;
    }

    public Backup getBackupConf() {
        return backupConf;
    }

    @Override
    public String toString() {
        return id + " " + date + " " + "(" + getBackupType() + ")" + " " + "[ " + this.getBackupConf().getBackupDir()
                + " ]";

    }

    public BackupType getBackupType() {
        return getBackupType(this.getBackupConf());
    }

    public static BackupType getBackupType(Backup backupConf) {
        Hdfs hdfs = backupConf.getHdfs();
        return (hdfs != null && hdfs.getUrl() != null && hdfs.getUrl().length() > 0) ? BackupType.HDFS
                : BackupType.LOCAL;
    }
}