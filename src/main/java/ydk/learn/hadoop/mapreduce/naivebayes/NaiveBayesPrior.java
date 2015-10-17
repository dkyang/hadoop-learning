package ydk.learn.hadoop.mapreduce.naivebayes;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import ydk.learn.hadoop.mapreduce.Constants;

import java.io.IOException;

/**
 * Created by yangdekun on 2015/10/17.
 */
public class NaiveBayesPrior {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();

        String inputPath = otherArgs[1];
        String outputPath = otherArgs[2];
        String countExampleFileName = otherArgs[3];

        // options
        configuration.set(Constants.COUNT_EXAMPLE_FILE_NAME, countExampleFileName);

        Job job = Job.getInstance(configuration, Constants.NAIVE_BAYES_PRIOR_JOB_NAME);
        job.setJarByClass(NaiveBayesPrior.class);
        job.setMapperClass(NaiveBayesPriorMapper.class);
        job.setCombinerClass(NaiveBayesPriorCombiner.class);
        job.setReducerClass(NaiveBayesPriorReducer.class);
        

    }
}
