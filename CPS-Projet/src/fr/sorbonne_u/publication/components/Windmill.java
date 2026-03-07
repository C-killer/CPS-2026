package fr.sorbonne_u.publication.components;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.messages.MessageFilter;
import fr.sorbonne_u.publication.Client;

/**
 * Windmill = subscriber client.
 * It subscribes to one channel with a filter and receives matching messages.
 */
public class Windmill extends Client {

	public Windmill(
			String receivingInboundURI,
			String brokerRegistrationInboundURI,
			RegistrationClass serviceClass,
			String channel,
			MessageFilterI filter) throws Exception {
		super(
				receivingInboundURI,
				brokerRegistrationInboundURI,
				serviceClass,
				channel,
				filter,
				null); // no publication message for windmill
	}

	/**
	 * Convenience constructor: FREE service + match-all filter.
	 */
	public Windmill(
			String receivingInboundURI,
			String brokerRegistrationInboundURI,
			String channel) throws Exception {
		super(
				receivingInboundURI,
				brokerRegistrationInboundURI,
				RegistrationClass.FREE,
				channel,
				new MessageFilter(null, null, null),
				null);
	}
}