package chapter02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 这个 Mapper 类是一个泛型类型，它有四个形参类型，分别指定 map 函数的输入键、输入值、输出键和输出值的类型。Hadoop 本身提供了一套可优化网络序
 * 列化传输的基本类型，而不直接使用 Java 内嵌的类型。这些类型都在 org.apache.hadoop.io 包中。LongWritable 类型相当于 Java 的 Long 类
 * 型、Text 类型相当于 Java 的 String 类型、IntWritable 类型相当于 Java 的 Integer 类型。
 */
public class MaxTemperatureMapper extends  Mapper<LongWritable, Text, Text, IntWritable> {
    private static final int MISSING = 9999;

    /**
     * map() 方法的输入是一个键和一个值。我们首先将包含有一行输入的 Text 值转换成 Java 的 String 类型，之后用 substring() 方法提取我们感
     * 兴趣的列。map() 方法还提供 Context 实例用于输出内容的写入。
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String year = line.substring(15, 19);
        int airTemperature;
        if (line.charAt(87) == '+') {
            airTemperature = Integer.parseInt(line.substring(88, 92));
        } else {
            airTemperature = Integer.parseInt(line.substring(87, 92));
        }
        String quality = line.substring(92, 93);
        if (airTemperature != MISSING && quality.matches("[01459]")) {
            // 将年份按 Text 对象进行读/写（因为我们把年份当作键），将气温值封装在 IntWritable 类型中。只有气温数据不缺并且对应质量代码显示
            // 为正确的气温读数时，这些数据才会被写入输出记录中。
            context.write(new Text(year), new IntWritable(airTemperature));
        }
    }
}