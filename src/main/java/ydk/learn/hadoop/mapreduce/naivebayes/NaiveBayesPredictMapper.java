package ydk.learn.hadoop.mapreduce.naivebayes;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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
// use distributedCache to transfer prior and likelihood file
public class NaiveBayesPredictMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Map<String, Integer> likelihoodMap = new HashMap<String, Integer>();
    private final Map<String, Integer> priorMap = new HashMap<String, Integer>();
    // 可能的label取值
    private static final String[] labelArray = {"0", "1", "2"};

    public NaiveBayesPredictMapper() {
        // 通过distributedCache传送
        File likelihoodFile = new File("likelihood.file");
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(likelihoodFile));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] keyValue = line.split("\t");
                likelihoodMap.put(keyValue[0], Integer.valueOf(keyValue[1]).intValue());
            }
        } catch (FileNotFoundException e1) {
            System.err.println("File not found: likelihood.file");
        } catch (IOException e2) {
            e2.printStackTrace();
        }

        File priorFile = new File("prior.file");
        try {
            reader = new BufferedReader(new FileReader(priorFile));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] keyValue = line.split("\t");
                priorMap.put(keyValue[0], Integer.valueOf(keyValue[1]).intValue());
            }
        } catch (FileNotFoundException e1) {
            System.err.println("File not found: prior.file");
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

        for (String label : labelArray) {

        }
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
