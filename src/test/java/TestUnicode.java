import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

public class TestUnicode {
    public static void main(String[] args) throws UnsupportedEncodingException {
        String str ="中国";
        String utf8 = new String(str.getBytes(StandardCharsets.UTF_8));
        System.out.println(utf8);

        String unicode = new String (str.getBytes(), StandardCharsets.UTF_8);
        System.out.println(unicode);
        String gbk = new String(unicode.getBytes("GBK"));
        System.out.println(gbk);
    }
}
