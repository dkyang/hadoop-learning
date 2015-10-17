package ydk.learn.hadoop.mapreduce.naivebayes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by yangdekun on 2015/10/16.
 */
public class NaiveBayesLikelihoodReducer extends Reducer<Text, IntWritable, Text, Text> {

    private static final Text resuableText = new Text();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }

        resuableText.set(String.valueOf(count));
        context.write(key, resuableText);
    }
}
