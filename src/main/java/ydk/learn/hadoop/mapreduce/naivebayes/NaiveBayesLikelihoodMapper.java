package ydk.learn.hadoop.mapreduce.naivebayes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ydk.learn.hadoop.mapreduce.Constants;
import ydk.learn.hadoop.mapreduce.Utils;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by yangdekun on 2015/10/16.
 */
// using for title classification
public class NaiveBayesLikelihoodMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final Text resuableText = new Text();
    private static final IntWritable oneInt = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        int pos = line.indexOf("\t");
        String label = line.substring(0, pos);
        StringTokenizer tokenizer = new StringTokenizer(line.substring(pos+1));
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            String outputKey = Utils.getLikelihoodKey(label, token);
            resuableText.set(outputKey);
            context.write(resuableText, oneInt);
        }
    }
}
