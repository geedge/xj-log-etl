public class Test {

    public static String intToIp(int ip) {
        StringBuilder sb = new StringBuilder();
        int num = 0;
        boolean needPoint = false; // 是否需要加入'.'
        for (int i = 0; i < 4; i++) {
            if (needPoint) {
                sb.append('.');
            }
            needPoint = true;
            int offset = 8 * (3 - i);
            num = (ip >> offset) & 0xff;
            sb.append(num);
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        People people = new People();
        if (2>1){
            people.setAge(2);
        }
        System.out.println(people.getAge());
    }
}
