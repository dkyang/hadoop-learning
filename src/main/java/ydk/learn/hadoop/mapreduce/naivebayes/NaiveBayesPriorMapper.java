package ydk.learn.hadoop.mapreduce.naivebayes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by yangdekun on 2015/10/17.
 */
public class NaiveBayesPriorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable oneInt = new IntWritable(1);
    private final Text reusableText = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        int pos = line.indexOf("\t");
        String label = line.substring(0, pos);

        reusableText.set(label);
        context.write(reusableText, oneInt);
    }
}
