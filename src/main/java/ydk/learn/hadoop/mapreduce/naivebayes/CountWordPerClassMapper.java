package ydk.learn.hadoop.mapreduce.naivebayes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by yangdekun on 2015/10/18
 */
public class CountWordPerClassMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final Text reusableText = new Text();
    private final IntWritable reusableInt = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        int pos = line.indexOf("\t");
        String label = line.substring(0, pos);

        StringTokenizer tokenizer = new StringTokenizer(line.substring(pos+1));
        int count = 0;
        while (tokenizer.hasMoreTokens()) {
            count++;
        }

        reusableText.set(label);
        reusableInt.set(count);
        context.write(reusableText, reusableInt);
    }
}
