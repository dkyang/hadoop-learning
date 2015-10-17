package ydk.learn.hadoop.mapreduce.naivebayes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by yangdekun on 2015/10/17.
 */
// 统计训练集中的所有样本数（求和），用于计算先验概率
// 将每一行映射为1
public class CountExampleMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final Text COMMON_KEY = new Text("example_sum");
    private static final IntWritable oneInt = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(COMMON_KEY, oneInt);
    }
}
