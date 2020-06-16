package chapter03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.net.URI;

/**
 * 将一个文件写入标准输出两次：在第一次写完之后，定位到文件的起始位置再次以流方式读取该文件并输出。
 */
public class FileSystemDoubleCat {
    public static void main(String[] args) throws Exception {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        FSDataInputStream in = null;
        try {
            // FileSystem 对象中的 open() 方法返回的是 FSDataInputStream 对象，而不是标准的 java.io 类对象。这个类是继承了
            // java.io.DataInputStream 的一个特殊类，并支持随机访问，由此可以从流的任意位置读取数据。
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
            in.seek(0); // seek() 可以移动到文件中任意一个绝对位置，该操作开销较高，需要慎重使用
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }
}