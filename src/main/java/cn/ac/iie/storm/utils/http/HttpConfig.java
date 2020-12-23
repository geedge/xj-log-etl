package cn.ac.iie.storm.utils.http;



public class HttpConfig {
    /**
     * 最大连接数400
     */

    private int maxConnectionNum;

    /**
     * 单路由最大连接数80
     */

    private int maxPerRoute;

    /**
     * 向服务端请求超时时间设置(单位:毫秒)
     */

    private int serverRequestTimeOut;

    /**
     * 向服务端连接超时时间设置(单位:毫秒)
     */

    private int serverConnectTimeOut;

    /**
     * 服务端响应超时时间设置(单位:毫秒)
     */

    private int serverResponseTimeOut;

    /**
     * Druid查询请求响应时间
     */

    private int druidSocketTimeOut;

    /**
     * Arango查询请求响应时间
     */

    private int arangoSocketTimeOut;

    /**
     * ClickHouse长查询请求响应时间
     */
    private int ckLongTermAccountSocketTimeOut;

    /**
     * ClickHouse实时查询请求响应时间
     */
    private int ckRealTimeAccountSocketTimeOut;

}
