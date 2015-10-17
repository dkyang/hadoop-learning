/**
 * Created by yangdekun on 2015/10/16.
 */
public class TestSubstring {
    public static void main(String[] args) {
        String line = "abc\twer";
        int pos = line.indexOf("\t");
        System.out.println(line.substring(0, pos) + ";");
        System.out.println(line.substring(pos+1));
    }
}
