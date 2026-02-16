package fr.sorbonne_u.publication;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.messages.MessageFilter;

public class Windmill extends Client {

	public Windmill(String receivingInboundURI,
			String brokerRegistrationInboundURI,
			RegistrationClass serviceClass,
			String channel,
			MessageFilterI filter) throws Exception {
		super(receivingInboundURI, brokerRegistrationInboundURI, serviceClass, channel, filter);
	}

	// Convenience constructor with a default "wind" filter
	public Windmill(String receivingInboundURI,
			String brokerRegistrationInboundURI,
			String channel) throws Exception {
		super(receivingInboundURI,
				brokerRegistrationInboundURI,
				RegistrationClass.FREE,
				channel,
				new MessageFilter(null, null, null)); // match all; refine later if you want
	}
}