package ydk.learn.hadoop.mapreduce.naivebayes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by yangdekun on 2015/10/17.
 */
// 待改名为SumCombiner，公用
public class NaiveBayesPriorCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable reusableInt = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }

        reusableInt.set(count);
        context.write(key, reusableInt);
    }
}
