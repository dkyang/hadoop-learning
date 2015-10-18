package ydk.learn.hadoop.mapreduce.naivebayes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import ydk.learn.hadoop.mapreduce.Constants;

/**
 * Created by yangdekun on 2015/10/18.
 */
public class NaiveBayesLikelihoodDriver {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        String inputPath = args[1];
        String outputPath = args[2];

        String jobName = Constants.NAIVE_BAYES_LIKELIHOOD_JOB_NAME;
        Job job = Job.getInstance(configuration, jobName);

        job.setJarByClass(NaiveBayesLikelihoodDriver.class);
        job.setMapperClass(NaiveBayesLikelihoodMapper.class);
        job.setReducerClass(NaiveBayesLikelihoodReducer.class);
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
