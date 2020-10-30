package tom.macondo.server.discovery;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(prefix = "zk", value = "enabled", havingValue = "false", matchIfMissing = true)
public class DummyDiscoveryService implements DiscoveryService {
	@EventListener(ApplicationReadyEvent.class)
	public void onApplicationEvent(ApplicationReadyEvent event) {

	}
}