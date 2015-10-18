package ydk.learn.hadoop.mapreduce.naivebayes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ydk.learn.hadoop.mapreduce.Constants;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yangdekun on 2015/10/16.
 */
public class NaiveBayesLikelihoodReducer extends Reducer<Text, IntWritable, Text, Text> {

    private static final Text reusableText = new Text();
    private final Map<String, Double> labelCountMap = new HashMap<String, Double>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String countWordByClassFileName = context
                                            .getConfiguration()
                                            .get(Constants.COUNT_WORD_BY_CLASS_FILE_NAME);
        File countWordByClassFile = new File(countWordByClassFileName);
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(countWordByClassFile));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] items = line.split("\t");
                labelCountMap.put(items[0], Double.valueOf(items[1]));
            }
        } catch (FileNotFoundException e1) {
            System.err.println("Count word by class file not found: " + countWordByClassFile);
        }
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        double count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }

        String keyString = key.toString();
        int pos = keyString.indexOf(Constants.KEY_CONNECT_CHAR);
        String label = keyString.substring(0, pos);
        double countWordByClass = labelCountMap.get(label);

        double likelihood = count / countWordByClass;

        reusableText.set(String.valueOf(likelihood));
        context.write(key, reusableText);
    }
}
