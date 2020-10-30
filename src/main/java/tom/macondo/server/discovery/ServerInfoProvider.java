package tom.macondo.server.discovery;

import tom.macondo.server.discovery.message.ServerInfo;

/**
 * @author: zhangchong
 * @Date: 2020/10/30 10:56
 **/
public interface ServerInfoProvider {
	String getServerId();

	ServerInfo getServerInfo();
}
