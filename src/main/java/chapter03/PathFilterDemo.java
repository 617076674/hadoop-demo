package chapter03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;

public class PathFilterDemo {
    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        Path path = new Path("hdfs://localhost/chapter02/*.txt");
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://localhost/"), configuration);
        FileStatus[] fileStatuses = fileSystem.globStatus(path, path1 -> false);
        Path[] paths = FileUtil.stat2Paths(fileStatuses);
        for (Path p : paths) {
            System.out.println(p);
        }
    }
}