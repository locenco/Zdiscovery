package tom.macondo.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.concurrent.CountDownLatch;

/**
 * @author: zhangchong
 * @Date: 2020/10/29 15:55
 **/
@SpringBootApplication
public class DiscoveryServiceApplication {
	static class DaemonAndShutdownHook {
		private final CountDownLatch keepAliveLatch = new CountDownLatch(1);
		private final Thread keepAliveThread;

		public DaemonAndShutdownHook() {
			keepAliveThread = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						System.out.println("开始等待");
						keepAliveLatch.await();
					} catch (InterruptedException e) {
						// bail out
					}
				}
			}, "elasticsearch[keepAlive/" + 1 + "]");
			keepAliveThread.setDaemon(false);
			// keep this thread alive (non daemon thread) until we shutdown
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					System.out.println("退出");
					keepAliveLatch.countDown();
				}
			});
			keepAliveThread.start();
		}

	}

	public static void main(String[] args) {
		SpringApplication.run(DiscoveryServiceApplication.class, args);
		new DaemonAndShutdownHook();
	}
}
