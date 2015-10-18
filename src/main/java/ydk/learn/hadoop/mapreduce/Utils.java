package ydk.learn.hadoop.mapreduce;

/**
 * Created by yangdekun on 2015/10/18.
 */
public class Utils {

    public static String getLikelihoodKey(String label, String token) {
        return label + Constants.KEY_CONNECT_CHAR + token;
    }
}
