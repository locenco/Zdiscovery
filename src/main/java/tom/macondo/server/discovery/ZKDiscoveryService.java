package tom.macondo.server.discovery;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.utils.CloseableUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * @author: zhangchong
 * @Date: 2020/10/29 14:39
 **/
@Slf4j
@ConditionalOnProperty(prefix = "zk", value = "enabled", havingValue = "true", matchIfMissing = false)
@Service
public class ZKDiscoveryService implements DiscoveryService, PathChildrenCacheListener {

	@Value("${zk.url}")
	private String zkUrl;
	@Value("${zk.retry_interval_ms}")
	private Integer zkRetryInterval;
	@Value("${zk.connection_timeout_ms}")
	private Integer zkConnectionTimeout;
	@Value("${zk.session_timeout_ms}")
	private Integer zkSessionTimeout;
	@Value("${zk.zk_dir}")
	private String zkDir;
	private CuratorFramework client;
	private String zkNodesDir;
	private PathChildrenCache cache;
	private volatile boolean stopped = true;

	@PostConstruct
	public void init(){
		//1. check parameters
		log.info("Initializing...");
		Assert.hasLength(zkUrl, missingProperty("zk.url"));
		Assert.notNull(zkRetryInterval, missingProperty("zk.retry_interval_ms"));
		Assert.notNull(zkConnectionTimeout, missingProperty("zk.connection_timeout_ms"));
		Assert.notNull(zkSessionTimeout, missingProperty("zk.session_timeout_ms"));

		log.info("Initializing discovery service using ZK connect string: {}", zkUrl);
		zkNodesDir = zkDir + "/nodes";
		initZkClient();
		//2. init
	}

	/**
	 * TODO　初始化zk客户端
	 */
	private void initZkClient() {
		try {
			client = CuratorFrameworkFactory.newClient(zkUrl, zkSessionTimeout, zkConnectionTimeout, new RetryForever(zkRetryInterval));
			client.start();
			client.blockUntilConnected();

			cache = new PathChildrenCache(client, zkNodesDir, true);
			cache.getListenable().addListener(this);
			cache.start();
			stopped = false;
			log.info("ZK client connected");
		} catch (Exception e) {
			log.error("Failed to connect to ZK: {}", e.getMessage(), e);
			CloseableUtils.closeQuietly(cache);
			CloseableUtils.closeQuietly(client);
			throw new RuntimeException(e);
		}
	}
	// TODO pulish this server
	// TODO get other server
	public static String missingProperty(String propertyName) {
		return "The " + propertyName + " property need to be set!";
	}

	public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
		if (stopped) {
			log.debug("Ignoring {}. Service is stopped.", pathChildrenCacheEvent);
			return;
		}
	}
	@PreDestroy
	public void destroy() {
		destroyZkClient();
		log.info("Stopped discovery service");
	}

	private void destroyZkClient() {
		stopped = true;
		CloseableUtils.closeQuietly(cache);
		CloseableUtils.closeQuietly(client);
		log.info("ZK client disconnected");
	}
}
