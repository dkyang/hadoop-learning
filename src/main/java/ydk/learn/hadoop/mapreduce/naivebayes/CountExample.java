package ydk.learn.hadoop.mapreduce.naivebayes;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by yangdekun on 2015/10/17.
 */
public class CountExample {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.out.println("Usage:CountExample <in> <out>");
            System.exit(2);
        }

        String inputPath = otherArgs[1];
        String outputPath = otherArgs[2];

        String jobName = "naive bayes count example";
        Job job = Job.getInstance(configuration, jobName);
        job.setJarByClass(CountExample.class);
        job.setMapperClass(CountExampleMapper.class);
        job.setCombinerClass(CountExampleCombiner.class);
        job.setReducerClass(CountExampleReducer.class);
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
