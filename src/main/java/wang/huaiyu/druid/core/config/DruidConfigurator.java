package wang.huaiyu.druid.core.config;

import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.http.client.HttpClient;
import io.druid.java.util.http.client.HttpClientConfig;
import io.druid.java.util.http.client.HttpClientInit;
import io.druid.java.util.http.client.Request;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import wang.huaiyu.druid.core.exception.DruidException;

import java.net.URL;

/**
 * io.druid 配置
 *
 * @author wang.huaiyu
 * @date 2019-06-19
 */
@Configuration
public class DruidConfigurator {

    /**
     * 主线程
     */
    @Value(value = "${druid.config.boss}")
    private Integer boss;

    /**
     * 工作线程
     */
    @Value(value = "${druid.config.worker}")
    private Integer worker;

    /**
     * 连接数
     */
    @Value(value = "${druid.config.connectionNumber}")
    private Integer connectionNumber;

    /**
     * 超时时长
     */
    @Value(value = "${druid.config.readTimeout}")
    private Integer readTimeout;

    /**
     * 未使用连接超时时长
     */
    @Value(value = "${druid.config.unusedConnectionTimeout}")
    private Integer unusedConnectionTimeout;

    /**
     * io.druid 客户端
     *
     * @return 客户端
     */
    @Bean(value = "druidClient")
    public HttpClient initClient() {
        this.initParameter();
        return HttpClientInit.createClient(
                HttpClientConfig.builder()
                        .withBossCount(boss)
                        .withWorkerCount(worker)
                        .withNumConnections(connectionNumber)
                        .withReadTimeout(Duration.millis(readTimeout))
                        .withUnusedConnectionTimeoutDuration(Duration.millis(unusedConnectionTimeout))
                        .build(),
                new Lifecycle()
        );
    }

    private final static Integer DEFAULT_BOSS = 1;
    private final static Integer DEFAULT_WORKER = Runtime.getRuntime().availableProcessors() * 2;
    private final static Integer DEFAULT_CONNECTION_NUMBER = 64;
    private final static Integer DEFAULT_READ_TIMEOUT = 60 * 1000;
    private final static Integer DEFAULT_UNUSED_CONNECTION_TIMEOUT = 15 * 1000;

    /**
     * 初始化参数
     */
    private void initParameter() {
        this.boss = null == boss || 0 == boss ? DEFAULT_BOSS : boss;
        this.worker = null == worker || 0 == worker ? DEFAULT_WORKER : worker;
        this.connectionNumber = null == connectionNumber || 0 == connectionNumber ? DEFAULT_CONNECTION_NUMBER : connectionNumber;
        this.readTimeout = null == readTimeout || 0 == readTimeout ? DEFAULT_READ_TIMEOUT : readTimeout;
        this.unusedConnectionTimeout = null == unusedConnectionTimeout || 0 == unusedConnectionTimeout ? DEFAULT_UNUSED_CONNECTION_TIMEOUT : unusedConnectionTimeout;
    }

    /**
     * http 请求地址
     */
    @Value(value = "${druid.http.url}")
    private String httpUrl;

    /**
     * 初始化请求对象
     *
     * @return 请求对象
     */
    @Bean(value = "druidRequest")
    public Request initRequest() {
        URL url;
        try {
            url = new URL(httpUrl);
        } catch (Exception e) {
            throw new DruidException("io.druid url can't read");
        }
        return new Request(HttpMethod.POST, url);
    }
}