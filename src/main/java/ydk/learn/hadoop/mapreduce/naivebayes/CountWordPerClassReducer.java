package ydk.learn.hadoop.mapreduce.naivebayes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by yangdekun on 2015/10/18.
 */
public class CountWordPerClassReducer extends Reducer<Text, IntWritable, Text, Text> {

    private static final Text reusableText = new Text();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }

        reusableText.set(String.valueOf(count));
        context.write(key, reusableText);
    }
}
