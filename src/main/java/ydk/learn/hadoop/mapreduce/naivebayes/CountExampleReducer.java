package ydk.learn.hadoop.mapreduce.naivebayes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by yangdekun on 2015/10/17.
 */
// 这里实际只有一个reducer，因为key都相同
public class CountExampleReducer extends Reducer<Text, IntWritable, Text, Text> {

    private final Text reusableText = new Text();

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
