package com.spark.utils;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Created by stefanie on 12/21/16.
 */
public class HadoopFileWriter {

    //private String pathString = "";
    private Configuration conf = null;

    private Path path = null;
    private FileSystem fs = null;
    private FSDataOutputStream out = null;

    public HadoopFileWriter(String filePath) throws IOException {

        //pathString = filePath;
        conf = new Configuration();
        fs = FileSystem.get(conf);
        path = new Path(filePath);
        out = fs.create(path);

    }

    public void writeLineToFile(String line) throws IOException {
        out.writeUTF(line);
        return;
    }

    public void closeFileWriter() throws IOException {
        fs.close();
    }

//    public static void main(String[] args) throws IOException {
//        Configuration conf = new Configuration();
//        FileSystem fs = FileSystem.get(conf);
//        Path path = new Path("/user/hadoop/hdfs/xxxx.txt");
//        FSDataOutputStream out = fs.create(path);
//        out.writeUTF("da jia hao,cai shi zhen de hao!");
//        fs.close();
//    }
}
