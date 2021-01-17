package cn.ac.iie.storm.utils.http;

import cn.ac.iie.storm.utils.config.StreamAggregateConfig;

import java.util.Random;

public class HttpGetUrl {

    public static String getPostUrl() {

        String[] split = StreamAggregateConfig.HTTP_POST_FLOODING_IP.split(",");
        Random random = new Random();
        int i = random.nextInt(split.length - 1);
        String url = "http://" + split[i] + ":" + StreamAggregateConfig.HTTP_POST_PORT + "/";
        return url;
    }


    public static String getHttpHost(String url) {
        String host = url.substring(7, 27);

        return host;
    }
}

