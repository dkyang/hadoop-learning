package ydk.learn.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by yangdekun on 2015/9/2.
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, Text> {
    private final static Text resuableText = new Text();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }

        System.out.println(key.toString() + count);
        resuableText.set(String.valueOf(count));
        context.write(key, resuableText);
    }
}
