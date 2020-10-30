package tom.macondo.server.discovery.message;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

/**
 * @author: zhangchong
 * @Date: 2020/10/30 10:55
 **/
@Data
@Builder
public class ServerInfo implements Serializable {
	String serverId;
	String ip;
	Integer port;
}
