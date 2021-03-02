package cn.ac.iie.storm.utils.hos;

import kafka.log.Log;

import java.io.*;


public class SendObject {
    public static void main(String[] args) throws IOException, IOException {
        //FileInputStream
        FileInputStream fileInputStream = new FileInputStream("src/resources/common_app_id_coding.properties");


        byte[] buffer = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] b = new byte[1024];
        int n;

        while ((n = fileInputStream.read(b)) != -1)
        {
            bos.write(b, 0, n);
        }
        fileInputStream.close();
        bos.close();
        buffer = bos.toByteArray();


        OutputStream out = new FileOutputStream("src/resources/1.txt");
        ByteArrayInputStream is = new ByteArrayInputStream(buffer);
        byte[] buf = new byte[1024];
        int len = 0;
        while((len=is.read(buf))!=-1){
            out.write(buf, 0, len);
        }
        is.close();
        out.close();
    }

    }

