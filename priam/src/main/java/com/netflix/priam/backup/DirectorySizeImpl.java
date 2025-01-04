package com.netflix.priam.backup;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;

/** Estimates remaining bytes or files to upload in a backup by looking at the file system */
public class DirectorySizeImpl implements DirectorySize {

    public long getBytes(String location, String... filters) {
        SummingFileVisitor fileVisitor = new SummingFileVisitor(filters);
        try {
            Files.walkFileTree(Paths.get(location), fileVisitor);
        } catch (IOException e) {
            // BackupFileVisitor is happy with an estimate and won't produce these in practice.
        }
        return fileVisitor.getTotalBytes();
    }

    public int getFiles(String location, String... filters) {
        SummingFileVisitor fileVisitor = new SummingFileVisitor(filters);
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
        private final String[] filters;

        public SummingFileVisitor(String... filters) {
            this.filters = filters;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
            if (!attrs.isRegularFile()) {
                return FileVisitResult.CONTINUE;
            }
            // This will return 0 if no filter is provided.
            if (Arrays.stream(filters).anyMatch(filter -> file.toString().contains(filter))) {
                totalBytes += attrs.size();
                totalFiles += 1;
            }
            return FileVisitResult.CONTINUE;
        }

        @Nonnull
        @Override
        public FileVisitResult visitFileFailed(Path file, IOException exc) {
            return FileVisitResult.CONTINUE;
        }

        @Nonnull
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
