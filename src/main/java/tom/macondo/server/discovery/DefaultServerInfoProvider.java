package tom.macondo.server.discovery;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import tom.macondo.server.discovery.message.ServerInfo;

import javax.annotation.PostConstruct;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author: zhangchong
 * @Date: 2020/10/30 10:58
 **/
@Component
@Slf4j
public class DefaultServerInfoProvider implements ServerInfoProvider {

	@Getter
	@Value("${service.id:#{null}}")
	private String serviceId;

	@Getter
	@Value("${service.ip:}")
	private String ip;

	@Getter
	@Value("${service.port:}")
	private Integer port;
	private ServerInfo serverInfo;

	@PostConstruct
	private void init() {
		if (StringUtils.isEmpty(serviceId)) {
			try {
				//获取本机的HostName作为serviceId
				serviceId = InetAddress.getLocalHost().getHostName();
				ip = InetAddress.getLocalHost().getHostAddress();
			} catch (UnknownHostException e) {
				serviceId = RandomStringUtils.randomAlphabetic(10);
				ip = "127.0.0.1";
			}


		}
		serverInfo = ServerInfo.builder()
				.serverId(serviceId)
				.ip(ip)
				.port(port)
				.build();
	}

	@Override
	public String getServerId() {
		return serviceId;
	}

	@Override
	public ServerInfo getServerInfo() {
		return serverInfo;
	}


}
