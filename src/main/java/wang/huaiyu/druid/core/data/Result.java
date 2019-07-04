package wang.huaiyu.druid.core.data;

import lombok.Builder;
import lombok.Data;
import org.joda.time.DateTime;

/**
 * io.druid 结果对象
 *
 * @param <T> 类型
 * @author wang.huaiyu
 * @date 2019-06-21
 */
@Data
@Builder
public class Result<T> {

    private DateTime timestamp;
    private T result;
}
