package com.spark.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by stefanie on 12/21/16.
 */
public class FileWriter {

    //private String pathString = "";
    private FileOutputStream out = null;

    public FileWriter(String filePath) throws IOException {

        out = new FileOutputStream(new File(filePath));
    }

    public void writeLineToFile(String line) throws IOException {
        out.write(line.getBytes());
        //out.write("\n".getBytes());
        return;
    }

    public void closeFileWriter() throws IOException {
        out.close();
    }

    public static void main(String[] args) throws IOException {
        FileWriter fw = new FileWriter("src/resources/");
    }
}

