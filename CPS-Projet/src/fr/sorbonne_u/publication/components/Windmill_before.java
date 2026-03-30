package fr.sorbonne_u.publication.components;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.messages.MessageFilter;

//代表系统中的消费者/订阅者,它订阅特定频道(如气象频道)并根据过滤器接收数据。
//It represents a consumer/subscriber in the system; it subscribes to a specific channel (such as a weather channel) and receives data based on filters.
/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class Windmill_before extends Client_before_plugins {

	protected Windmill_before(String receivingInboundURI,
			String brokerRegistrationInboundURI,
			RegistrationClass serviceClass,
			String channel,
			MessageFilterI filter) throws Exception {
		super(receivingInboundURI, brokerRegistrationInboundURI, serviceClass, channel, filter);
	}

	// Convenience constructor with a default "wind" filter
	protected Windmill_before(String receivingInboundURI,
			String brokerRegistrationInboundURI,
			String channel) throws Exception {
		super(receivingInboundURI,
				brokerRegistrationInboundURI,
				RegistrationClass.FREE,
				channel,
				new MessageFilter(null, null, null)); // match all; refine later if you want
	}
}