import static cn.ac.iie.storm.utils.config.GetMap.getDy;

public class TestDy {
    public static void main(String[] args) {
        String private_ip = getDy("Private IP");
        System.out.println(private_ip);
    }
}
