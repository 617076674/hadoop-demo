package chapter03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * 该范例显示了如何将本地文件复制到 Hadoop 文件系统。每次 Hadoop 调用 progress() 方法时，也就是每次将 64KB 数据包写入 datanode
 * 管线后，打印一个时间点来显示整个运行过程。
 */
public class FileCopyWithProgress {
    public static void main(String[] args) throws Exception {
        String localSrc = args[0];
        String dst = args[1];
        InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        // FileSystem 类有一系列新建文件的方法。最简单的方法是给准备建的文件指定一个 Path 对象，然后返回一个写入数据的输出流。
        // create() 方法能够为需要写入且当前不存在的文件创建父目录。尽管这样很方便，但有时并不希望这样。如果希望父目录不存在就
        // 导致文件写入失败，则应该先调用 exists() 方法检查父目录是否存在。另一种方案是使用 FileContext，允许你可以控制是否
        // 创建父目录。
        // Progressable 接口可以把数据写入 datanode 的进度通知给应用
        OutputStream out = fs.create(new Path(dst), new Progressable() {
            public void progress() {
                System.out.print(".");
            }
        });
        IOUtils.copyBytes(in, out, 4096, true);
    }
}
