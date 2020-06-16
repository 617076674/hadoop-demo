package chapter02;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 负责运行 MapReduce 作业。
 */
public class MaxTemperature {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperature <input path> <output path>");
            System.exit(-1);
        }
        // Job 对象指定作业执行规范。我们可以用它来控制整个作业的运行。我们在 Hadoop 集群上运行这个作业时，要把代码打包成一个 JAR 文件
        // （Hadoop 在集群上发布这个文件）。比不明确指定 JAR 文件的名称，在 Job 对象的setJarByClass() 方法中传递一个类即可，Hadoop
        // 利用这个类来查找包含它的 JAR 文件，进而找到相关的 JAR 文件。
        Job job = new Job();
        job.setJarByClass(MaxTemperature.class);
        job.setJobName("Max temperature");
        // 构造 Job 对象之后，需要指定输入和输出数据的路径。调用 FileInputFormat 类的静态方法 addInputPath() 来定义输入数据的路径，这个
        // 路径可以是单个的文件、一个目录（此时，将目录下所有文件当作输入）或符合特定文件模式的一系列文件。由函数名可知，可以多次调用
        // addInputPath() 来实现多路径的输入。
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // 调用 FileOutputFormat 类中的静态方法 setOutputPath() 来指定输出路径（只能有一次输出路径）。这个方法指定的是 reduce 函数输出
        // 文件的写入目录。在运行作业前该目录是不应该存在的，否则 Hadoop 会报错并拒绝运行作业。这种预防措施的目的是防止数据丢失。
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 通过 setMapperClass() 和 setReducerClass() 方法指定要用的 map 类型和 reduce 类型。
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        // setOutputKeyClass() 和 setOutputValueClass() 方法控制 reduce 函数的输出类型，并且必须和 Reduce 类产生的相匹配。map 函数
        // 的输出类型默认情况下和 reduce 函数是相同的，因此如果 mapper 产生出和 reducer 相同的类型时（如同本例所示），不需要单独设置。但是，
        // 如果不同，则必须通过 setMapOutputKeyClass() 和 setMapOutputValueClass() 方法来设置 map 函数的输出类型。
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 在设置定义 map 和 reduce 函数的类之后，可以开始运行作业。Job 中的 waitForCompletion() 方法提交作业并等待执行完成。该方法的唯
        // 一参数是一个标识，指示是否已经生成详细输出。当标识为 true 时，作业会把其进度信息写到控制台。waitForCompletion() 方法返回一个布尔
        // 值，表示执行的成（true）败（false），这个布尔值被转换成程序的退出代码 0 或者 1。
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}