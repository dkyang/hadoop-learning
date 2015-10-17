package ydk.learn.hadoop.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import ydk.learn.hadoop.mapreduce.Constants;

import java.io.IOException;

/**
 * Created by yangdekun on 2015/9/8.
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        for (String arg : otherArgs) {
            System.out.println(arg);
        }

        if (otherArgs.length != 3) {
            System.out.println("Usage:wordcount <in> <out>");
            System.exit(2);
        }

        String inputPath = otherArgs[1];
        String outputPath = otherArgs[2];

        String jobName = Constants.WORD_COUNT_JOB_NAME;
        Job job = Job.getInstance(configuration, jobName);
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPriority(JobPriority.HIGH);

        // job.setOutputFormatClass(OutputFormat.class);

        // 设置输入、输出路径
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }
}
