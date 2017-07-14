/**
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.backup;

import com.netflix.priam.utils.DateUtil;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Date;

/**
 * POJO to encapsulate the metadata for a snapshot
 * Created by aagrawal on 1/31/17.
 */

final public class BackupMetadata implements Serializable {
    private String snapshotDate;
    private String token;
    private Date start, completed;
    private Status status;
    private String snapshotLocation;

    /**
     * Enum to describe the status of the snapshot
     */
    public enum Status {
        /**
         * Denotes snapshot has started successfully.
         */
        STARTED,
        /**
         * Denotes snapshot has uploaded successfully.
         */
        FINISHED,
        /**
         * Denotes snapshot has failed to upload successfully or there was a failure marking the snapshot as failure.
         */
        FAILED
    }

    public BackupMetadata(String token, Date start) throws Exception {
        if (start == null || token == null || StringUtils.isEmpty(token))
            throw new Exception(String.format("Invalid Input: Token: {} or start date:{} is null or empty.", token, start));

        this.snapshotDate = DateUtil.formatyyyyMMdd(start);
        this.token = token;
        this.start = start;
        this.status = Status.STARTED;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;

        BackupMetadata that = (BackupMetadata) o;

        if (!this.snapshotDate.equals(that.snapshotDate)) return false;
        if (!this.token.equals(that.token)) return false;
        return this.start.equals(that.start);
    }

    @Override
    public int hashCode() {
        int result = this.snapshotDate.hashCode();
        result = 31 * result + this.token.hashCode();
        result = 31 * result + this.start.hashCode();
        return result;
    }

    /**
     * Get the snapshot date formatted in yyyyMMdd.
     *
     * @return snapshot date formatted in yyyyMMdd.
     */
    public String getSnapshotDate() {
        return this.snapshotDate;
    }

    /**
     * Get the token for which snapshot was initiated.
     *
     * @return snapshot token.
     */
    public String getToken() {
        return this.token;
    }

    /**
     * Get the start date on which snapshot was initiated.
     *
     * @return start date of snapshot.
     */
    public Date getStart() {
        return this.start;
    }

    /**
     * Get the date on which snapshot was marked as finished/failed etc.
     *
     * @return completion date of snapshot.
     */

    public Date getCompleted() {
        return this.completed;
    }

    /**
     * Get the status of the snapshot.
     *
     * @return snapshot status
     */
    public BackupMetadata.Status getStatus() {
        return this.status;
    }

    /**
     * Set the completion date of snashot status.
     *
     * @param completed date of completion for a snapshot.
     */
    public void setCompleted(Date completed) {
        this.completed = completed;
    }

    /**
     * Set the status of the snapshot.
     *
     * @param status of the snapshot.
     */
    public void setStatus(Status status) {
        this.status = status;
    }

    /**
     * Get the snapshot location where snapshot is uploaded.
     *
     * @return snapshot upload location for the meta file.
     */
    public String getSnapshotLocation() {
        return this.snapshotLocation;
    }

    /**
     * Set the snapshot location where snapshot is uploaded.
     *
     * @param snapshotLocation where snapshot meta file is uploaded.
     */
    public void setSnapshotLocation(String snapshotLocation) {
        this.snapshotLocation = snapshotLocation;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("BackupMetadata{");
        sb.append("snapshotDate='").append(snapshotDate).append('\'');
        sb.append(", token='").append(token).append('\'');
        sb.append(", start=").append(start);
        sb.append(", completed=").append(completed);
        sb.append(", status=").append(status);
        sb.append(", snapshotLocation=").append(snapshotLocation);
        sb.append('}');
        return sb.toString();
    }
}
