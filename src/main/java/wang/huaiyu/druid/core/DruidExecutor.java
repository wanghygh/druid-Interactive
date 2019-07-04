package wang.huaiyu.druid.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.http.client.HttpClient;
import io.druid.java.util.http.client.Request;
import io.druid.java.util.http.client.response.ToStringResponseHandler;
import io.druid.query.Query;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.scan.ScanResultValue;
import org.joda.time.Duration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import wang.huaiyu.druid.core.data.Result;
import wang.huaiyu.druid.core.exception.DruidException;

import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * io.druid 查询执行器
 *
 * @author wang.huaiyu
 * @date 2019-06-21
 */
@Component
public class DruidExecutor {

    private HttpClient client;
    private Request request;
    private DefaultObjectMapper mapper;

    @Autowired
    public DruidExecutor(@Qualifier(value = "druidClient") HttpClient client, @Qualifier(value = "druidRequest") Request request) {
        this.client = client;
        this.request = request;
        this.mapper = new DefaultObjectMapper();
    }

    /**
     * 执行(返回单结果)
     *
     * @param query       查询条件
     * @param returnClass 返回类
     * @param <T>         对象
     * @return 结果<T>对象 (io.druid 很坑，自带的 io.druid.query.Result 对象中结果对象是 value，引擎返回的是 result)
     */
    public <T> Result<T> get(Query query, Class<T> returnClass) {
        return this.find(query, returnClass).get(0);
    }

    /**
     * 执行(返回结果集)
     *
     * @param query       查询条件
     * @param returnClass 返回类
     * @param <T>         对象
     * @return 结果<T>对象
     */
    public <T> List<Result<T>> find(Query query, Class<T> returnClass) {
        try {
            String jsonString = mapper.writeValueAsString(query);
            request.setContent(jsonString.getBytes());
            ListenableFuture<String> future = client.go(request, new ToStringResponseHandler(Charset.forName("UTF-8")), Duration.millis(3000));
            String result = future.get(3000, TimeUnit.MILLISECONDS);
            // 扫描
            if (ScanResultValue.class.getName().equals(returnClass.getName())
                    // 元数据查询
                    || SegmentAnalysis.class.getName().equals(returnClass.getName())
                    // 分组自定义命名字段
                    || JSONObject.class.getName().equals(returnClass.getName())) {
                List<T> ts = JSON.parseObject(result, new TypeReference<List<T>>() {
                });
                return ts.stream().map(t -> Result.<T>builder()
                        .result(t)
                        .build())
                        .collect(Collectors.toList());
            }
            return JSON.parseObject(result, new TypeReference<List<Result<T>>>() {
            });
        } catch (Exception e) {
            throw new DruidException(String.format("occur an error when io.druid executing -> %s", e.getMessage()));
        }
    }
}
