import java.io.*;

import static cn.ac.iie.storm.utils.config.GetMap.getDy;
import static cn.ac.iie.storm.utils.config.GetMap.getSzd;
import static cn.ac.iie.storm.utils.http.HttpAsncClient.getConnAnsyHttpPost;

public class TestHos {
    public static void main(String[] args) throws IOException {
        InputStream input = new FileInputStream(new File("src/resources/test"));

        byte[] buffer = null;

        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        byte[] b = new byte[1024];

        int n;

        while ((n = input.read(b)) != -1)
        {
            bos.write(b, 0, n);
        }

        input.close();

        bos.close();
        buffer = bos.toByteArray();
        getConnAnsyHttpPost("http://10.111.151.217:8888/yd-test/testfile?method=put",buffer);
    }
}
