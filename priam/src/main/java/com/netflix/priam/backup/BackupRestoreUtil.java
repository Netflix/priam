/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.netflix.priam.backup;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.netflix.priam.backupv2.IMetaProxy;
import com.netflix.priam.utils.DateUtil;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Provider;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

/** Helper methods applicable to both backup and restore */
public class BackupRestoreUtil {
    private static final Pattern columnFamilyFilterPattern = Pattern.compile(".\\..");
    private final Map<String, List<String>> includeFilter;
    private final Map<String, List<String>> excludeFilter;

    private static final Map<String, List<String>> FILTER_COLUMN_FAMILY =
            ImmutableMap.of(
                    "system",
                    Arrays.asList(
                            "local",
                            "peers",
                            "peers_v2",
                            "hints",
                            "compactions_in_progress",
                            "LocationInfo"));

    @Inject
    public BackupRestoreUtil(String configIncludeFilter, String configExcludeFilter) {
        includeFilter = getFilter(configIncludeFilter);
        excludeFilter = getFilter(configExcludeFilter);
    }

    public static Optional<AbstractBackupPath> getLatestValidMetaPath(
            IMetaProxy metaProxy, DateUtil.DateRange dateRange) {
        return metaProxy
                .findMetaFiles(dateRange)
                .stream()
                .filter(meta -> metaProxy.isMetaFileValid(meta).valid)
                .findFirst();
    }

    public static List<AbstractBackupPath> getMostRecentSnapshotPaths(
            AbstractBackupPath latestValidMetaFile,
            IMetaProxy metaProxy,
            Provider<AbstractBackupPath> pathProvider)
            throws Exception {
        Path metaFile = metaProxy.downloadMetaFile(latestValidMetaFile);
        List<AbstractBackupPath> snapshotPaths =
                metaProxy
                        .getSSTFilesFromMeta(metaFile)
                        .stream()
                        .map(
                                value -> {
                                    AbstractBackupPath path = pathProvider.get();
                                    path.parseRemote(value);
                                    return path;
                                })
                        .collect(Collectors.toList());
        FileUtils.deleteQuietly(metaFile.toFile());
        return snapshotPaths;
    }

    public static Map<String, List<String>> getFilter(String inputFilter)
            throws IllegalArgumentException {
        if (StringUtils.isEmpty(inputFilter)) return null;
        final Map<String, List<String>> columnFamilyFilter = new HashMap<>();
        String[] filters = inputFilter.split(",");
        for (String cfFilter : filters) {
            if (columnFamilyFilterPattern.matcher(cfFilter).find()) {
                String[] filter = cfFilter.split("\\.");
                String keyspaceName = filter[0];
                String columnFamilyName = filter[1];
                if (columnFamilyName.contains("-"))
                    columnFamilyName = columnFamilyName.substring(0, columnFamilyName.indexOf("-"));
                List<String> existingCfs =
                        columnFamilyFilter.getOrDefault(keyspaceName, new ArrayList<>());
                if (!columnFamilyName.equalsIgnoreCase("*")) existingCfs.add(columnFamilyName);
                columnFamilyFilter.put(keyspaceName, existingCfs);
            } else {
                throw new IllegalArgumentException(
                        "Invalid format: " + cfFilter + ". \"keyspace.columnfamily\" is required.");
            }
        }
        return columnFamilyFilter;
    }

    /**
     * Returns if provided keyspace and/or columnfamily is filtered for backup or restore.
     *
     * @param keyspace name of the keyspace in consideration
     * @param columnFamilyDir name of the columnfamily directory in consideration
     * @return true if directory should be filter from processing; otherwise, false.
     */
    public final boolean isFiltered(String keyspace, String columnFamilyDir) {
        if (StringUtils.isEmpty(keyspace) || StringUtils.isEmpty(columnFamilyDir)) return false;
        String columnFamilyName = columnFamilyDir.split("-")[0];
        if (FILTER_COLUMN_FAMILY.containsKey(keyspace)
                && FILTER_COLUMN_FAMILY.get(keyspace).contains(columnFamilyName)) return true;
        if (excludeFilter != null)
            if (excludeFilter.containsKey(keyspace)
                    && (excludeFilter.get(keyspace).isEmpty()
                            || excludeFilter.get(keyspace).contains(columnFamilyName))) {
                return true;
            }
        if (includeFilter != null)
            return !(includeFilter.containsKey(keyspace)
                    && (includeFilter.get(keyspace).isEmpty()
                            || includeFilter.get(keyspace).contains(columnFamilyName)));
        return false;
    }

    /**
     * Get all the backup directories for Cassandra.
     *
     * @param dataDirectory the location of the data folder.
     * @param monitoringFolder folder where cassandra backup's are configured.
     * @return Set of the path(s) containing the backup folder for each columnfamily.
     * @throws Exception incase of IOException.
     */
    public static ImmutableSet<Path> getBackupDirectories(
            String dataDirectory, String monitoringFolder) throws Exception {
        ImmutableSet.Builder<Path> backupPaths = ImmutableSet.builder();
        Path dataPath = Paths.get(dataDirectory);
        if (Files.exists(dataPath) && Files.isDirectory(dataPath))
            try (DirectoryStream<Path> directoryStream =
                    Files.newDirectoryStream(dataPath, path -> Files.isDirectory(path))) {
                for (Path keyspaceDirPath : directoryStream) {
                    try (DirectoryStream<Path> keyspaceStream =
                            Files.newDirectoryStream(
                                    keyspaceDirPath, path -> Files.isDirectory(path))) {
                        for (Path columnfamilyDirPath : keyspaceStream) {
                            Path backupDirPath =
                                    Paths.get(columnfamilyDirPath.toString(), monitoringFolder);
                            if (Files.exists(backupDirPath) && Files.isDirectory(backupDirPath)) {
                                backupPaths.add(backupDirPath);
                            }
                        }
                    }
                }
            }
        return backupPaths.build();
    }
}
