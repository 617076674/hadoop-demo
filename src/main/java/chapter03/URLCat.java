package chapter03;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URL;

public class URLCat {
    static {
        // 为了让 Java 程序能够识别 Hadoop 的 hdfs URL 方案，这里采用的方法是通过 FsUrlStreamHandlerFactory 实例调用
        // java.net.URL 对象的 setURLStreamHandlerFactory() 方法。每个 Java 虚拟机只能调用一次这个方法，因此通常在静
        // 态方法中调用。这个限制意味着如果程序的其他组件（如不受你控制的第三方组件）已经声明一个 URLStreamHandlerFactory
        // 实例，你将无法使用这种方法从 Hadoop 中读取数据。
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    /**
     * 调用 Hadoop 中简洁的 IOUtils 类，并在 finally 子句中关闭数据流，同时也可以在输入流和输出流之间复制数据（本例为 System.out）。
     */
    public static void main(String[] args) throws Exception {
        InputStream in = null;
        try {
            in = new URL(args[0]).openStream();
            // copyBytes 方法的最后两个参数，第一个设置用于复制的缓冲区大小，第二个设置复制结束后是否关闭数据流。
            // 这里我们选择在 finally 子句中自行关闭输入流，因而 System.out 不必关闭输入流。
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }
}

