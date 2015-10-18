package ydk.learn.hadoop.mapreduce.naivebayes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ydk.learn.hadoop.mapreduce.Constants;
import ydk.learn.hadoop.mapreduce.Utils;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Created by yangdekun on 2015/10/17.
 */
// the format of input file is:
// sku_id'\t'title
// use distributedCache to send prior and likelihood file
public class NaiveBayesPredictMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text outputKeyText = new Text();
    private final Text outputValText = new Text();
    private final Map<String, Integer> likelihoodMap = new HashMap<String, Integer>();
    private final Map<String, Integer> priorMap = new HashMap<String, Integer>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        String likelihoodFileName = configuration.get(Constants.LIKELIHOOD_FILE_NAME);
        String priorFileName = configuration.get(Constants.PRIOR_FILE_NAME);

        // 通过distributedCache传送
        File likelihoodFile = new File(likelihoodFileName);
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(likelihoodFile));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] keyValue = line.split("\t");
                likelihoodMap.put(keyValue[0], Integer.valueOf(keyValue[1]).intValue());
            }
        } catch (FileNotFoundException e1) {
            System.err.println("File not found: " + likelihoodFileName);
        } catch (IOException e2) {
            e2.printStackTrace();
        }

        File priorFile = new File(priorFileName);
        try {
            reader = new BufferedReader(new FileReader(priorFile));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] keyValue = line.split("\t");
                priorMap.put(keyValue[0], Integer.valueOf(keyValue[1]).intValue());
            }
        } catch (FileNotFoundException e1) {
            System.err.println("File not found: " + priorFileName);
        } catch (IOException e2) {
            e2.printStackTrace();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] items = line.split("\t");
        String skuId = items[0];
        String title = items[1];

        ArrayList<String> tokens = getTokens(title);

        double maxPosterior = Double.MIN_VALUE;
        String labelPredict = Constants.LABEL_ARRAY[0];
        for (String label : Constants.LABEL_ARRAY) {
            double posterior = 0.0;

            // TODO(yangdekun): 判断非空
            double prior = priorMap.get(label);
            posterior += Math.log(prior);

            for (String token : tokens) {
                String likelihoodKey = Utils.getLikelihoodKey(label, token);
                double likelihood = likelihoodMap.get(likelihoodKey);
                posterior += Math.log(likelihood);
            }

            if (posterior > maxPosterior) {
                maxPosterior = posterior;
                labelPredict = label;
            }
        }

        outputKeyText.set(skuId);
        outputValText.set(labelPredict);
        context.write(outputKeyText, outputValText);
    }

    private ArrayList getTokens(String str) {
        ArrayList<String> tokens = new ArrayList<String>();
        StringTokenizer tokenizer = new StringTokenizer(str);
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            tokens.add(token);
        }

        return tokens;
    }
}
