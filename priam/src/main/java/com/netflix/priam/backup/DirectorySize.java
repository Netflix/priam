package com.netflix.priam.backup;

import com.google.inject.ImplementedBy;

/** estimates the number of bytes and files remaining to upload in a snapshot/backup */
@ImplementedBy(DirectorySizeImpl.class)
public interface DirectorySize {
    /** return the total bytes of all snapshot/backup files south of location in the filesystem */
    long getBytes(String location, String... filters);
    /** return the total files of all snapshot/backup files south of location in the filesystem */
    int getFiles(String location, String... filters);
}
