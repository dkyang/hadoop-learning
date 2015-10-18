package ydk.learn.hadoop.mapreduce;

/**
 * Created by yangdekun on 2015/10/17.
 */
public class Constants {
    // word count
    public static final String WORD_COUNT_JOB_NAME = "word count";

    // naive bayes
    public static final String COUNT_EXAMPLE_JOB_NAME = "naive bayes count example";
    public static final String COUNT_EXAMPLE_FILE_NAME = "count example file name";

    public static final String NAIVE_BAYES_PRIOR_JOB_NAME = "naive bayes prior";

    public static final String NAIVE_BAYES_LIKELIHOOD_JOB_NAME = "naive bayes likelihood";

    public static final String COUNT_WORD_BY_CLASS_JOB_NAME = "count word by class";
    public static final String COUNT_WORD_BY_CLASS_FILE_NAME = "count word by class file";

    public static final String KEY_CONNECT_CHAR = "_";

    // 可能的label取值
    public static final String[] LABEL_ARRAY = {"0", "1", "2"};

    public static final String LIKELIHOOD_FILE_NAME = "likelihood file";
    public static final String PRIOR_FILE_NAME = "prior file";

    public static final String NAIVE_BAYES_PREDICT_JOB_NAME = "naive bayes predict";

}
