package wang.huaiyu.druid.core.exception;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * io.druid 异常对象
 *
 * @author wang.huaiyu
 * @date 2019-06-19
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DruidException extends RuntimeException {

    /**
     * 消息
     */
    private String message;
}
