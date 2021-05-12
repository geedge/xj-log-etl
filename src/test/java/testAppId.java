import java.io.IOException;

import static cn.ac.iie.storm.utils.config.GetMap.getJstxDetail_id;

public class testAppId {
    public static void main(String[] args) throws IOException {
        String jstxDetail_id = getJstxDetail_id(100100);
        System.out.println(jstxDetail_id);
    }

}
