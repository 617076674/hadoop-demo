package chapter03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URI;

/**
 * 如果程序的其他组件（如不受你控制的第三方组件）已经声明一个 URLStreamHandlerFactory 实例，你将无法使用 hdfs URL 方案从 Hadoop 中
 * 读取数据。在这种情况下，我们需要用 FileSystem API 来打开一个文件的输入流。
 */
public class FileSystemCat {
    public static void main(String[] args) throws Exception {
        String uri = args[0];
        // Configuration 对象封装了客户端或服务器的配置，通过设置配置文件读取类路径来实现（如etc/hadoop/core-site.xml）。
        Configuration conf = new Configuration();
        // FileSystem 是一个通用的文件系统 API，所以第一步是检索我们需要使用的文件系统实例，这里是 HDFS。
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        InputStream in = null;
        try {
            // Hadoop 文件系统中通过 Hadoop Path 对象（而非 java.io.File 对象，因为它的语义与本地文件系统联系太紧密）来代表文件。可以将
            // 路径视为一个 Hadoop 文件系统 URI，如 hdfs://localhost/user/tom/quangle.txt
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
