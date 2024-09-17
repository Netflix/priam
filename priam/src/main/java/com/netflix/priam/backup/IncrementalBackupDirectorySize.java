package com.netflix.priam.backup;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

/** Estimates remaining bytes or files to upload in a backup by looking at the file system */
public class IncrementalBackupDirectorySize implements DirectorySize {

    public long getBytes(String location) {
        SummingFileVisitor fileVisitor = new SummingFileVisitor();
        try {
            Files.walkFileTree(Paths.get(location), fileVisitor);
        } catch (IOException e) {
            // BackupFileVisitor is happy with an estimate and won't produce these in practice.
        }
        return fileVisitor.getTotalBytes();
    }

    public int getFiles(String location) {
        SummingFileVisitor fileVisitor = new SummingFileVisitor();
        try {
            Files.walkFileTree(Paths.get(location), fileVisitor);
        } catch (IOException e) {
            // BackupFileVisitor is happy with an estimate and won't produce these in practice.
        }
        return fileVisitor.getTotalFiles();
    }

    private static final class SummingFileVisitor implements FileVisitor<Path> {
        private long totalBytes;
        private int totalFiles;

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
            if (file.toString().contains(AbstractBackup.INCREMENTAL_BACKUP_FOLDER) && attrs.isRegularFile()) {
                totalBytes += attrs.size();
                totalFiles += 1;
            }
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path file, IOException exc) {
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
            return FileVisitResult.CONTINUE;
        }

        long getTotalBytes() {
            return totalBytes;
        }

        int getTotalFiles() {
            return totalFiles;
        }
    }
}
