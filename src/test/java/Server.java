import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Server {

    public static void main(String args[]) throws IOException {
        //为了简单起见，所有的异常信息都往外抛
        try{
            //创建一个服务器对象，端口3333
            ServerSocket serverSocket=new ServerSocket(10080);
            //创建一个客户端对象，这里的作用是用作多线程，必经服务器服务的不是一个客户端
            Socket client=null;
            boolean flag=true;

            while(flag){
                System.out.println("服务器已启动，等待客户端请求。。。。");
                //accept是阻塞式方法，对新手来说这里很有可能出错，下面的注意事项我会说到
                SimpleDateFormat d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
                System.out.println(d.format(new Date()));// new Date()为获取当前系统时间
                client=serverSocket.accept();
                //创建一个线程，每个客户端对应一个线程
            }
            client.close();
            serverSocket.close();
            System.out.println("服务器已关闭。");
        }catch(Exception e){
            e.printStackTrace();
        }

    }
}



