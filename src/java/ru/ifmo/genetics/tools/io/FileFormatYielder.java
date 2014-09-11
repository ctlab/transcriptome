package ru.ifmo.genetics.tools.io;

import ru.ifmo.genetics.utils.tool.values.InValue;
import ru.ifmo.genetics.utils.tool.values.Yielder;

import java.io.File;

public class FileFormatYielder extends Yielder<String> {

    InValue<File> file;

    public FileFormatYielder(InValue<File> file) {
        this.file = file;
    }

    @Override
    public String yield() {
        File f = file.get();
        if (f == null) {
            return null;
        }

        String fileName = f.getName().toLowerCase();
        if (fileName.endsWith(".binq")) {
            return "binq";
        }
        else if (fileName.endsWith(".fq") || fileName.endsWith(".fastq")) {
            return "fastq";
        } else if (fileName.endsWith(".fasta") || fileName.endsWith(".fa") || fileName.endsWith(".fn")) {
            return "fasta";
        }
        throw new RuntimeException("Can't detect file format for file " + fileName);
    }

    @Override
    public String description() {
        return "determines format based on file extension";
    }
}
