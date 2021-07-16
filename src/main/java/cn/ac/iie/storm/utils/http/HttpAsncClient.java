package cn.ac.iie.storm.utils.http;

import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import com.google.common.collect.Maps;
import org.apache.http.*;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;

import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.protocol.HTTP;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class HttpAsncClient {

    //全局连接池对象

    private final static Logger log = Logger.getLogger(HttpAsncClient.class);

    /**
     * 获取Http客户端连接对象
     *
     * @param
     * @return Http客户端连接对象
     */
    public static CloseableHttpAsyncClient getHttpAsynClient() {
        // 创建Http请求配置参数

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(StreamAggregateConfig.S3_CONNECTIONREQUESTTIMEOUT)
                .setConnectTimeout(StreamAggregateConfig.S3_CONNECTTIMEOUT)
                .setSocketTimeout(StreamAggregateConfig.S3_SOKETTIMEOUT)
                .build();
        /**
         * 初始化连接池信息
         */
//        //配置io线程
//        IOReactorConfig ioReactorConfig = IOReactorConfig.custom().
//                setIoThreadCount(Runtime.getRuntime().availableProcessors())
//                .setSoKeepAlive(true)
//                .build();
//        //设置连接池大小
//        ConnectingIOReactor ioReactor = null;
//        try {
//            ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);
//        } catch (IOReactorException e) {
//            e.printStackTrace();
//        }
//        PoolingNHttpClientConnectionManager connManager = new PoolingNHttpClientConnectionManager(ioReactor);
//        connManager.setMaxTotal(2000);//最大连接数设置1
//        connManager.setDefaultMaxPerRoute(400);//per route最大连接数设置
//
//        log.info("Initializing PoolingHttpClientConnectionManager Complete");

        /**
         * 测出超时重试机制为了防止超时不生效而设置
         *  如果直接放回false,不重试
         *  这里会根据情况进行判断是否重试
         */

//        ConnectionKeepAliveStrategy myStrategy = (response, context) -> {
//            HeaderElementIterator it = new BasicHeaderElementIterator
//                    (response.headerIterator(HTTP.CONN_KEEP_ALIVE));
//            while (it.hasNext()) {
//                HeaderElement he = it.nextElement();
//                String param = he.getName();
//                String value = he.getValue();
//                if (value != null && param.equalsIgnoreCase("timeout")) {
//                    return Long.parseLong(value) * 1000;
//                }
//            }
//            return 10;//如果没有约定，则默认定义时长为60s
//        };

        // 创建httpClient
        return HttpAsyncClients.custom()
                // 把请求相关的超时信息设置到连接客户端
                .setDefaultRequestConfig(requestConfig)
                // 把请求重试设置到连接客户端
//                .setKeepAliveStrategy(myStrategy)
//                .setConnectionManager(connManager)
//                // 配置连接池管理对象
                .build();

    }


    public static int getConnAnsyHttpPost(String url, byte[] s) throws UnsupportedEncodingException {


        final CountDownLatch latch = new CountDownLatch(1);
            Map<String, String> resultMap = Maps.newHashMap();

            HttpPost request = new HttpPost(url);

            request.setHeader("User-Name",StreamAggregateConfig.S3_HEARDER_USERNAME);
            request.setHeader("Password",StreamAggregateConfig.S3_HEARDER_PASSWORD);
            request.setHeader("File-Type",StreamAggregateConfig.S3_HEARDER_FILETYPE);
            request.setHeader("File-Title",StreamAggregateConfig.S3_HEARDER_FILETITLE);
            request.setHeader("Is-Fulltex",StreamAggregateConfig.S3_HEARDER_ISFULLTEX);
            request.setHeader("Content-Type",StreamAggregateConfig.S3_HEARDER_CONTENTTYPE);
        final int[] statusCode = new int[1];

            request.setEntity(new ByteArrayEntity(s));
            CloseableHttpAsyncClient httpAsynClient = getHttpAsynClient();
            httpAsynClient.start();

            httpAsynClient.execute(request, new FutureCallback<HttpResponse>() {

                @Override
                public void completed(final HttpResponse response) {
                    latch.countDown();
                    statusCode[0] = response.getStatusLine().getStatusCode();
                    if (statusCode[0] ==200){
                        log.info(request.getRequestLine() +"文件发送成功，状态码为"+"->" + statusCode[0]);
                    }else {
                        log.error(request.getRequestLine() +"文件发送失败，状态码为"+"->" + statusCode[0]);
                    }

                }
                @Override
                public void failed(Exception ex) {
                    latch.countDown();
                    log.error(request.getRequestLine() + "->" + ex);
                    log.error(request.getEntity()+"-----------"+ex);
                }

                @Override
                public void cancelled() {
                    latch.countDown();
                    log.error(request.getRequestLine() + " cancelled");
                }
            });
            try {
                latch.await();

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                httpAsynClient.close();
            } catch (IOException ignore) {
            }

        return statusCode[0];
    }
}