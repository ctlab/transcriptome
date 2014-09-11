package ru.ifmo.genetics.utils;

import org.jetbrains.annotations.NotNull;
import ru.ifmo.genetics.utils.tool.values.PathInValue;

import java.io.*;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileUtils {
    public static void createOrClearDir(File dir) {
        //noinspection ResultOfMethodCallIgnored
        dir.mkdirs();
        clearDir(dir);
    }

    public static void createOrClearDir(PathInValue dir) {
        createOrClearDir(dir.get());
    }

    public static void createOrClearDirRecursively(File dir) {
        //noinspection ResultOfMethodCallIgnored
        dir.mkdirs();
        clearDirRecursively(dir);
    }

    public static void createOrClearDirRecursively(PathInValue dir) {
        createOrClearDirRecursively(dir.get());
    }

    /**
     * Clears directory: removes only files in it!
     */
    public static void clearDir(File dir) {
        for (File f : dir.listFiles()) {
            if (!f.isDirectory()) {
                f.delete();
            }
        }
    }

    /**
     * Clears directory: removes all it's content
     */
    public static void clearDirRecursively(File dir) {
        for (File f : dir.listFiles()) {
            if (f.isDirectory()) {
                removeDirRecursively(f);
            } else {
                //noinspection ResultOfMethodCallIgnored
                f.delete();
            }
        }
    }

    /**
     * Removes directory and all it's content
     */
    public static void removeDirRecursively(File dir) {
        for (File f : dir.listFiles()) {
            if (f.isDirectory()) {
                removeDirRecursively(f);
            } else {
                //noinspection ResultOfMethodCallIgnored
                f.delete();
            }
        }
        //noinspection ResultOfMethodCallIgnored
        dir.delete();
    }

    /**
     * Finds files that match regexp in dir and all subdirs and removes them.
     */
    public static void findAndRemoveRecursively(File dir, String regexp) {
        for (File f : dir.listFiles()) {
            if (f.isDirectory()) {
                findAndRemoveRecursively(dir, regexp);
            } else {
                if (f.getName().matches(regexp)) {
                    //noinspection ResultOfMethodCallIgnored
                    f.delete();
                }
            }
        }
    }


    public static long fileSizeByName(String fileName) throws IOException {
        FileInputStream fileIn = new FileInputStream(fileName);
        long res = fileIn.getChannel().size();
        fileIn.close();
        return res;
    }

    public static long filesSizeByNames(Iterable<String> fileNames) throws IOException {
        long res = 0;
        for (String fileName: fileNames) {
            res += fileSizeByName(fileName);
        }
        return res;
    }

    public static long filesSizeByNames(String[] fileNames) throws IOException {
        return filesSizeByNames(Arrays.asList(fileNames));
    }

    public static long fileSize(File file) throws IOException {
        FileInputStream fileIn = new FileInputStream(file);
        long res = fileIn.getChannel().size();
        fileIn.close();
        return res;
    }

    public static long filesSize(Iterable<File> files) throws IOException {
        long res = 0;
        for (File file : files) {
            res += fileSize(file);
        }
        return res;
    }

    public static long filesSize(File[] files) throws IOException {
        return filesSize(Arrays.asList(files));
    }

    private static Pattern baseNamePattern = Pattern.compile("^(.*)\\.[^\\.]*$");
    public static String baseName(File file) {
        String name = file.getName();
        Matcher matcher = baseNamePattern.matcher(name);
        if (matcher.matches()) {
            return matcher.group(1);
        } else {
            return name;
        }
    }


    public static long linesNumber(File file) throws IOException {
        long linesNumber = 0;
        final int BUF_SIZE = 1 << 15;   // 32 Kb
        byte[] buf = new byte[BUF_SIZE];

        InputStream is = new FileInputStream(file);
        while (true) {
            int read = is.read(buf);
            if (read == -1) {
                break;
            }
            for (int i = 0; i < read; ++i) {
                if (buf[i] == '\n') {
                    linesNumber++;
                }
            }
        }
        is.close();

        return linesNumber;
    }

    public static String removeExtension(String s, String... extensions) {
        for (String extension: extensions) {
            if (!extension.startsWith(".")) {
                extension = "." + extension;
            }

            if (s.toLowerCase().endsWith(extension.toLowerCase())) {
                return s.substring(0, s.length() - extension.length());
            }
        }
        return s;
    }

}
