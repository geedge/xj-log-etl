import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

public class TestUFT {
    public static void main(String[] args) throws UnsupportedEncodingException {
     String str = "�й�";
     System.out.println(new String( str.getBytes("GBK") , StandardCharsets.UTF_8));
    }

}
