/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.backup;

import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.PriamServer;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.Sleeper;
import org.apache.cassandra.io.sstable.SSTableLoaderWrapper;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.streaming.PendingFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.regex.Pattern;

/*
 * Incremental SSTable Restore using SSTable Loader
 */
@Singleton
public class IncrementalRestore extends AbstractRestore
{
    private static final Logger logger = LoggerFactory.getLogger(IncrementalRestore.class);
    public static final String JOBNAME = "INCR_RESTORE_THREAD";

    /* Marked public for testing */
    public static final Pattern SECONDRY_INDEX_PATTERN = Pattern.compile("^[a-zA-Z_0-9-]+\\.[a-zA-Z_0-9-]+\\.[a-z1-9]{2,4}$");

    private final File restoreDir;
    
    @Inject
    private SSTableLoaderWrapper loader;
    
    @Inject
    private PriamServer priamServer;

    @Inject
    public IncrementalRestore(IConfiguration config, Sleeper sleeper)
    {
        super(config, JOBNAME, sleeper);
        this.restoreDir = new File(config.getDataFileLocation(), "restore_incremental");
    }

    @Override
    public void execute() throws Exception
    {
        String prefix = config.getRestorePrefix();
        if (Strings.isNullOrEmpty(prefix))
        {
            logger.error("Restore prefix is not set, skipping incremental restore to avoid looping over the incremental backups. Plz check the configurations");
            return; // No point in restoring the files which was just backedup.
        }

        if (config.isRestoreClosestToken())
        {
            priamServer.getId().getInstance().setToken(restoreToken.toString());
        }

        Date start = tracker.first().time;
        Iterator<AbstractBackupPath> incrementals = fs.list(prefix, start, Calendar.getInstance().getTime());
        FileUtils.createDirectory(restoreDir); // create restore dir.
        while (incrementals.hasNext())
        {
            AbstractBackupPath temp = incrementals.next();
            if (tracker.contains(temp) || start.compareTo(temp.time) >= 0)
                continue; // ignore the ones which where already downloaded.
            if (temp.getType() != BackupFileType.SST)
                continue; // download SST's only.
            // skip System Keyspace, else you will run into concurrent schema issues.
            if (temp.getKeyspace().equalsIgnoreCase("System"))
                continue;
            /* Cassandra will rebuild Secondary index's after streaming is complete so we can ignore those */
            if (SECONDRY_INDEX_PATTERN.matcher(temp.fileName).matches()) // Make this use the constant from 1.1
                continue;
            File keyspaceDir = new File(restoreDir, temp.keyspace);
            FileUtils.createDirectory(keyspaceDir);
            download(temp, new File(keyspaceDir, temp.fileName));
        }
        // wait for all the downloads in this batch to complete.
        waitToComplete();
        // stream the SST's in the dir
        for (File keyspaceDir : restoreDir.listFiles())
        {
            Collection<PendingFile> streamedSSTs = loader.stream(keyspaceDir);
            // cleanup the dir which where streamed.
            loader.deleteCompleted(streamedSSTs);
        }
    }

    /**
     * Run every 20 Sec
     */
    public static TaskTimer getTimer()
    {
        return new SimpleTimer(JOBNAME, 20L * 1000);
    }

    @Override
    public String getName()
    {
        return JOBNAME;
    }

}
