package tom.macondo.server.discovery;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.SerializationUtils;
import tom.macondo.server.discovery.message.ServerInfo;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type.CHILD_REMOVED;

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
	private String nodePath;
	private String zkNodesDir;
	private PathChildrenCache cache;
	private volatile boolean stopped = true;

	private ExecutorService reconnectExecutorService;

	@Autowired
	private ServerInfoProvider serverInfoProvider;


	@PostConstruct
	public void init() {
		//1. check parameters
		log.info("Initializing...");
		Assert.hasLength(zkUrl, missingProperty("zk.url"));
		Assert.notNull(zkRetryInterval, missingProperty("zk.retry_interval_ms"));
		Assert.notNull(zkConnectionTimeout, missingProperty("zk.connection_timeout_ms"));
		Assert.notNull(zkSessionTimeout, missingProperty("zk.session_timeout_ms"));

		reconnectExecutorService = Executors.newSingleThreadExecutor();

		log.info("Initializing discovery service using ZK connect string: {}", zkUrl);
		zkNodesDir = zkDir + "/nodes";
		initZkClient();
		//2. init
	}

	@EventListener(ApplicationReadyEvent.class)
	public void onApplicationEvent(ApplicationReadyEvent event) {
		if (stopped) {
			log.debug("Ignoring application ready event. Service is stopped.");
			return;
		} else {
			log.info("Received application ready event. Starting current ZK node.");
		}
		if (client.getState() != CuratorFrameworkState.STARTED) {
			log.debug("Ignoring application ready event, ZK client is not started, ZK client state [{}]", client.getState());
			return;
		}
		publishCurrentServer();
		//TODO update network topology
		getOtherServers().forEach(System.out::println);
	}

	/**
	 * 　初始化zk客户端
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
	// get other server

	/**
	 * 服务发现，通过PathChildrenCacheEvent事件监听实现
	 *
	 * @return
	 */
	private List<ServerInfo> getOtherServers() {
		return cache.getCurrentData().stream()
				.filter(cd -> !cd.getPath().equals(nodePath))
				.map(cd -> {
					try {
						return (ServerInfo) SerializationUtils.deserialize(cd.getData());
					} catch (NoSuchElementException e) {
						log.error("Failed to decode ZK node", e);
						throw new RuntimeException(e);
					}
				})
				.collect(Collectors.toList());
	}

	/**
	 * 处理重连
	 */
	private volatile boolean reconnectInProgress = false;

	private synchronized void reconnect() {
		if (!reconnectInProgress) {
			reconnectInProgress = true;
			try {
				destroyZkClient();
				initZkClient();
				publishCurrentServer();
			} catch (Exception e) {
				log.error("Failed to reconnect to ZK: {}", e.getMessage(), e);
			} finally {
				reconnectInProgress = false;
			}
		}
	}

	/**
	 * 服务注册
	 */
	private synchronized void publishCurrentServer() {
		ServerInfo serverInfo = serverInfoProvider.getServerInfo();
		//当前服务是否已经存在？
		//存在后新建 并设置监听器重连

		if (currentServerExists()) {
			log.info("[{}] ZK node for current server already exists. NOT created new one: {}", serverInfo.getServerId(), nodePath);
		} else {
			log.info("[{}] Creating ZK node for current instance", serverInfo.getServerId());
			try {
				nodePath = client.create().creatingParentsIfNeeded()
						.withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
						.forPath(zkNodesDir + "/", SerializationUtils.serialize(serverInfo));
				log.info("[{}] Created ZK node for current instance: {}", serverInfo.getServerId(), nodePath);
				client.getConnectionStateListenable().addListener((client, newState) -> {
					log.info("[{}] ZK state changed: {}", serverInfo.getServerId(), newState);
					if (newState == ConnectionState.LOST) {
						reconnectExecutorService.submit(this::reconnect);
					}
				});
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public boolean currentServerExists() {
		if (nodePath == null) {
			return false;
		}
		try {
			ServerInfo serverInfo = serverInfoProvider.getServerInfo();
			ServerInfo registeredServerInfo = null;
			registeredServerInfo = (ServerInfo) SerializationUtils.deserialize(client.getData().forPath(nodePath));
			if (serverInfo.equals(registeredServerInfo)) {
				return true;
			}
		} catch (KeeperException.NoNodeException e) {
			log.info("ZK node does not exist: {}", nodePath);
		} catch (Exception e) {
			log.error("Couldn't check if ZK node exists", e);
		}
		return false;
	}

	public static String missingProperty(String propertyName) {
		return "The " + propertyName + " property need to be set!";
	}

	@Override
	public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
		if (stopped) {
			log.debug("Ignoring {}. Service is stopped.", pathChildrenCacheEvent);
			return;
		}
		if (client.getState() != CuratorFrameworkState.STARTED) {
			log.debug("Ignoring {}, ZK client is not started, ZK client state [{}]", pathChildrenCacheEvent, client.getState());
			return;
		}
		ChildData data = pathChildrenCacheEvent.getData();
		if (data == null) {
			log.debug("Ignoring {} due to empty child data", pathChildrenCacheEvent);
			return;
		} else if (data.getData() == null) {
			log.debug("Ignoring {} due to empty child's data", pathChildrenCacheEvent);
			return;
		} else if (nodePath != null && nodePath.equals(data.getPath())) {
			if (pathChildrenCacheEvent.getType() == CHILD_REMOVED) {
				log.info("ZK node for current instance is somehow deleted.");
				publishCurrentServer();
			}
			log.debug("Ignoring event about current server {}", pathChildrenCacheEvent);
			return;
		}
		ServerInfo instance = (ServerInfo) SerializationUtils.deserialize(data.getData());
		log.info("Processing [{}] event for [{}]", pathChildrenCacheEvent.getType(), instance.getServerId());
	}


	@PreDestroy
	public void destroy() {
		destroyZkClient();
		reconnectExecutorService.shutdownNow();
		log.info("Stopped discovery service");
	}

	private void destroyZkClient() {
		stopped = true;
		try {
			unpublishCurrentServer();
		} catch (Exception e) {
		}
		CloseableUtils.closeQuietly(cache);
		CloseableUtils.closeQuietly(client);
		log.info("ZK client disconnected");
	}

	private void unpublishCurrentServer() {
		try {
			if (nodePath != null) {
				client.delete().forPath(nodePath);
			}
		} catch (Exception e) {
			log.error("Failed to delete ZK node {}", nodePath, e);
			throw new RuntimeException(e);
		}
	}
}
