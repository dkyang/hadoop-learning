package ydk.learn.hadoop.mapreduce.naivebayes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import ydk.learn.hadoop.mapreduce.Constants;

/**
 * Created by yangdekun on 2015/10/18.
 */
public class CountWordPerClassDriver {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.out.println("Usage:CountWordPerClassDriver <in> <out>");
            System.exit(2);
        }

        String inputPath = otherArgs[1];
        String outputPath = otherArgs[2];

        String jobName = Constants.COUNT_WORD_BY_CLASS_JOB_NAME;
        Job job = Job.getInstance(configuration, jobName);
        job.setJarByClass(CountWordPerClassDriver.class);
        job.setMapperClass(CountWordPerClassMapper.class);
//        job.setCombinerClass(CountExampleCombiner.class);
        job.setReducerClass(CountWordPerClassReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPriority(JobPriority.HIGH);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }
}
