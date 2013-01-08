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
package com.netflix.priam.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.HintedHandOffManager;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.Restore;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.scheduler.TaskTimer;

@Singleton
public class TuneCassandra extends Task
{
    public static final String JOBNAME = "Tune-Cassandra";
    private static final Logger logger = LoggerFactory.getLogger(HintedHandOffManager.class);

    @Inject
    public TuneCassandra(IConfiguration config)
    {
        super(config);
    }

    /**
     * update the cassandra yaml file.
     */
    // there is no way we can have uncheck with snake's implementation.
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void updateYaml(IConfiguration config, String yamlLocation, String hostname, String seedProvider) throws IOException
    {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Yaml yaml = new Yaml(options);
        File yamlFile = new File(yamlLocation);
        Map map = (Map) yaml.load(new FileInputStream(yamlFile));
        map.put("cluster_name", config.getAppName());
        map.put("storage_port", config.getStoragePort());
        map.put("ssl_storage_port", config.getSSLStoragePort());
        map.put("rpc_port", config.getThriftPort());
        map.put("listen_address", hostname);
        map.put("rpc_address", hostname);
        //Dont bootstrap in restore mode
        map.put("auto_bootstrap", !Restore.isRestoreEnabled(config));
        map.put("saved_caches_directory", config.getCacheLocation());
        map.put("commitlog_directory", config.getCommitLogLocation());
        map.put("data_file_directories", Lists.newArrayList(config.getDataFileLocation()));
        boolean enableIncremental = (config.getBackupHour() >= 0 && config.isIncrBackup()) && (CollectionUtils.isEmpty(config.getBackupRacs()) || config.getBackupRacs().contains(config.getRac()));
        map.put("incremental_backups", enableIncremental);
        map.put("endpoint_snitch", config.getSnitch());
        map.put("in_memory_compaction_limit_in_mb", config.getInMemoryCompactionLimit());
        map.put("compaction_throughput_mb_per_sec", config.getCompactionThroughput());
	    map.put("partitioner", derivePartitioner(map.get("partitioner").toString(), config.getPartitioner()));
        
        map.put("memtable_total_space_in_mb", config.getMemtableTotalSpaceMB());
        map.put("stream_throughput_outbound_megabits_per_sec", config.getStreamingThroughputMB());

        map.put("multithreaded_compaction", config.getMultithreadedCompaction());

        map.put("max_hint_window_in_ms", config.getMaxHintWindowInMS());
        map.put("hinted_handoff_throttle_in_kb", config.getHintedHandoffThrottleKb());
        map.put("max_hints_delivery_threads", config.getMaxHintThreads());

        List<?> seedp = (List) map.get("seed_provider");
        Map<String, String> m = (Map<String, String>) seedp.get(0);
        m.put("class_name", seedProvider);

        configureGlobalCaches(config, map);
        map.put("num_tokens", config.getNumTokens());

        logger.info(yaml.dump(map));
        yaml.dump(map, new FileWriter(yamlFile));
    }

    /**
     * Setup the cassandra global cache values
     */
    private static void configureGlobalCaches(IConfiguration config, Map yaml)
    {
        final String keyCacheSize = config.getKeyCacheSizeInMB();
        if(keyCacheSize != null)
        {
            yaml.put("key_cache_size_in_mb", Integer.valueOf(keyCacheSize));

            final String keyCount = config.getKeyCacheKeysToSave();
            if(keyCount != null)
                yaml.put("key_cache_keys_to_save", Integer.valueOf(keyCount));
        }

        final String rowCacheSize = config.getRowCacheSizeInMB();
        if(rowCacheSize != null)
        {
            yaml.put("row_cache_size_in_mb", Integer.valueOf(rowCacheSize));

            final String rowCount = config.getRowCacheKeysToSave();
            if(rowCount != null)
                yaml.put("row_cache_keys_to_save", Integer.valueOf(rowCount));
        }
    }

    static String derivePartitioner(String fromYaml, String fromConfig)
    {
        if(fromYaml == null || fromYaml.isEmpty())
            return fromConfig;
        //this check is to prevent against overwriting an existing yaml file that has
        // a partitioner not RandomPartitioner or (as of cass 1.2) Murmur3Partitioner.
        //basically we don't want to hose existing deployments by changing the partitioner unexpectedly on them
        final String lowerCase = fromYaml.toLowerCase();
        if(lowerCase.contains("randomparti") || lowerCase.contains("murmur"))
            return fromConfig;
        return fromYaml;
    }

    @SuppressWarnings("unchecked")
    public void updateYaml(boolean autobootstrap) throws IOException
    {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Yaml yaml = new Yaml(options);
        File yamlFile = new File(config.getCassHome() + "/conf/cassandra.yaml");
        @SuppressWarnings("rawtypes")
        Map map = (Map) yaml.load(new FileInputStream(yamlFile));
        //Dont bootstrap in restore mode
        map.put("auto_bootstrap", autobootstrap);
        logger.info("Updating yaml" + yaml.dump(map));
        yaml.dump(map, new FileWriter(yamlFile));
    }

    @Override
    public void execute() throws IOException
    {
        TuneCassandra.updateYaml(config, config.getCassHome() + "/conf/cassandra.yaml", null, config.getSeedProviderName());
    }

    @Override
    public String getName()
    {
        return "Tune-Cassandra";
    }

    public static TaskTimer getTimer()
    {
        return new SimpleTimer(JOBNAME);
    }
}
