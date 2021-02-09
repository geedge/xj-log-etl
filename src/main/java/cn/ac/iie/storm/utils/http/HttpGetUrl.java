package cn.ac.iie.storm.utils.http;

import cn.ac.iie.storm.utils.config.StreamAggregateConfig;

import java.util.Random;

public class HttpGetUrl {

    public static String getPostUrl() {

        String[] split = StreamAggregateConfig.HTTP_POST_FLOODING_IP.split(",");
        Random random = new Random();
        int i = random.nextInt(split.length);
        String url = "http://" + split[i] + ":" + StreamAggregateConfig.HTTP_POST_PORT + "/";
        return url;
    }


    public static String getHttpHost(String urlString) {
        int index = urlString.indexOf("://");
        if (index != -1) {
            urlString = urlString.substring(index + 3);
        }
        index = urlString.indexOf("/");
        if (index != -1) {
            urlString = urlString.substring(0, index);
        }
        return urlString;
    }
}

