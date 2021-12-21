import java.io.IOException;

import static cn.ac.iie.storm.utils.general.GetMap.getDy;


public class TestVpn {
    public static void main(String[] args) throws IOException {
        String dy = getDy("Norrköping,Östergötland,Sweden");
        System.out.println(dy);
    }
}

