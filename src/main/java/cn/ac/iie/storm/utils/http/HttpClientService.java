package cn.ac.iie.storm.utils.http;


import com.google.common.collect.Maps;
import org.apache.http.*;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.UnknownHostException;
import java.util.Map;


public class HttpClientService {

    HttpConfig httpConfig;

    //全局连接池对象
    private PoolingHttpClientConnectionManager connectionManager;
    private final static Logger log = Logger.getLogger(HttpClientService.class);
    /**
     * 初始化连接池信息
     */
    public void initConnectionManager() {
        if (connectionManager == null) {
            connectionManager = new PoolingHttpClientConnectionManager();
            // 整个连接池最大连接数
           // connectionManager.setMaxTotal(httpConfig.getMaxConnectionNum());
            connectionManager.setMaxTotal(400);
            // 每路由最大连接数，默认值是2
          //  connectionManager.setDefaultMaxPerRoute(httpConfig.getMaxPerRoute());
            connectionManager.setDefaultMaxPerRoute(80);
        }
        log.info("Initializing PoolingHttpClientConnectionManager Complete");
    }

    /**
     * 获取Http客户端连接对象
     *
     * @param socketTimeOut 响应超时时间
     * @return Http客户端连接对象
     */
    public CloseableHttpClient getHttpClient(int socketTimeOut) {
        // 创建Http请求配置参数
        RequestConfig requestConfig = RequestConfig.custom()
                // 获取连接超时时间
                //.setConnectionRequestTimeout(httpConfig.getServerRequestTimeOut())
                .setConnectionRequestTimeout(10)
                // 请求超时时间
//                .setConnectTimeout(httpConfig.getServerConnectTimeOut())
                .setConnectTimeout(10)
                // 响应超时时间
                .setSocketTimeout(socketTimeOut)
                .build();

        /**
         * 测出超时重试机制为了防止超时不生效而设置
         *  如果直接放回false,不重试
         *  这里会根据情况进行判断是否重试
         */
        HttpRequestRetryHandler retry = (exception, executionCount, context) -> {
            if (executionCount >= 3) {// 如果已经重试了3次，就放弃
                return false;
            }
            if (exception instanceof NoHttpResponseException) {// 如果服务器丢掉了连接，那么就重试
                return true;
            }
            if (exception instanceof SSLHandshakeException) {// 不要重试SSL握手异常
                return false;
            }
            if (exception instanceof InterruptedIOException) {// 超时
                return true;
            }
            if (exception instanceof UnknownHostException) {// 目标服务器不可达
                return false;
            }
            if (exception instanceof ConnectTimeoutException) {// 连接被拒绝
                return false;
            }
            if (exception instanceof SSLException) {// ssl握手异常
                return false;
            }
            HttpClientContext clientContext = HttpClientContext.adapt(context);
            HttpRequest request = clientContext.getRequest();
            // 如果请求是幂等的，就再次尝试
            if (!(request instanceof HttpEntityEnclosingRequest)) {
                return true;
            }
            return false;
        };


        ConnectionKeepAliveStrategy myStrategy = (response, context) -> {
            HeaderElementIterator it = new BasicHeaderElementIterator
                    (response.headerIterator(HTTP.CONN_KEEP_ALIVE));
            while (it.hasNext()) {
                HeaderElement he = it.nextElement();
                String param = he.getName();
                String value = he.getValue();
                if (value != null && param.equalsIgnoreCase("timeout")) {
                    return Long.parseLong(value) * 1000;
                }
            }
            return 60 * 1000;//如果没有约定，则默认定义时长为60s
        };

        // 创建httpClient
        return HttpClients.custom()
                // 把请求相关的超时信息设置到连接客户端
                .setDefaultRequestConfig(requestConfig)
                // 把请求重试设置到连接客户端
                .setRetryHandler(retry)
                .setKeepAliveStrategy(myStrategy)
                // 配置连接池管理对象
                .setConnectionManager(connectionManager)
                .build();
    }





    /**
     * @param url:请求地址
     *
     * @param socketTimeOut:响应超时时间
     **/
    public Map<String, String> gethttpFloodingPost(String url, byte[] s, int socketTimeOut) {
        Map<String, String> resultMap = Maps.newHashMap();
        // 创建GET请求对象
        CloseableHttpResponse response = null;
        try {
            HttpPost httpPost = new HttpPost(url);
            //头部消息
            httpPost.setHeader("Content-Type", "utf-8");
            httpPost.setHeader("User","user");
            httpPost.setHeader("Password","111111");
            httpPost.setHeader("Topic","t_ip_gkzb");
            httpPost.setHeader("Format","avro");
            httpPost.setHeader("Host","127.0.0.1:10080");
            httpPost.setHeader("Conection","Keep-Alilve");
            httpPost.setEntity(new ByteArrayEntity(s));
            response = getHttpClient(socketTimeOut).execute(httpPost);
            // 获取响应实体
            HttpEntity entity = response.getEntity();
            // 获取响应信息
            resultMap.put("status", String.valueOf(response.getStatusLine().getStatusCode()));
            resultMap.put("result", EntityUtils.toString(entity, "UTF-8"));
        } catch (ClientProtocolException e) {
            log.error("协议错误: {}"+e.getMessage() );
            resultMap.put("status", String.valueOf(HttpStatus.SC_SERVICE_UNAVAILABLE));
            resultMap.put("message", e.getMessage());
        } catch (ParseException e) {
            log.error("解析错误: {}"+ e.getMessage());
            resultMap.put("status", String.valueOf(HttpStatus.SC_SERVICE_UNAVAILABLE));
            resultMap.put("message", e.getMessage());
        } catch (IOException e) {
            log.error("IO错误: {}"+ e.getMessage());
            resultMap.put("status", String.valueOf(HttpStatus.SC_BAD_GATEWAY));
            resultMap.put("message", e.getMessage());
        } catch (Exception e) {
            log.error("其它错误: {}"+e.getMessage());
            resultMap.put("status", String.valueOf(HttpStatus.SC_INTERNAL_SERVER_ERROR));
            resultMap.put("message", e.getMessage());
        } finally {
            if (null != response) {
                try {
                    EntityUtils.consumeQuietly(response.getEntity());
                    response.close();
                } catch (IOException e) {
                    log.error("释放链接错误: {}"+e.getMessage());
                }
            }
        }
        return resultMap;
    }

    public static void main(String[] args) {
    }
}

