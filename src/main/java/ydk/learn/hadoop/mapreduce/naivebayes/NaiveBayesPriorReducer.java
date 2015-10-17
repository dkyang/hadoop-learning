package ydk.learn.hadoop.mapreduce.naivebayes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ydk.learn.hadoop.mapreduce.Constants;

import java.io.*;

/**
 * Created by yangdekun on 2015/10/17.
 */
public class NaiveBayesPriorReducer extends Reducer<Text, IntWritable, Text, Text> {

    private final Text reusableText = new Text();
    private double exampleNumber = 0.0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        // Mapper或Reducer读取形参的方法
        // 通过distributedCache传入文件
        String countExampleFileName = context.getConfiguration().get(Constants.COUNT_EXAMPLE_FILE_NAME);
        File countExampleFile = new File(countExampleFileName);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(countExampleFile));
            String line = reader.readLine();
            String[] items = line.split("\t");
            exampleNumber = Double.valueOf(items[1]);
        } catch (FileNotFoundException e1) {
            System.err.println("Count Example file not found: " + countExampleFileName);
        }
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        double count = 0.0;
        for (IntWritable value : values) {
            count += value.get();
        }

        double prior = count / exampleNumber;

        reusableText.set(String.valueOf(prior));
        context.write(key, reusableText);
    }
}
