package chapter03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class DeleteFile {
    public static void main(String[] args) throws IOException {
        Path path = new Path("hdfs://localhost/tmp");
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost/"), new Configuration());
        System.out.println(fs.delete(path, true));
    }
}
