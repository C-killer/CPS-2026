package fr.sorbonne_u.publication.components;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.publication.Client;

/**
 * MeteoStation = publisher client.
 * It publishes one weather message to a channel.
 */
public class MeteoStation extends Client {

	public MeteoStation(
			String receivingInboundURI,
			String brokerRegistrationInboundURI,
			RegistrationClass serviceClass,
			String channel,
			MessageI messageToPublish) throws Exception {
		super(
				receivingInboundURI,
				brokerRegistrationInboundURI,
				serviceClass,
				channel,
				null, // no subscription filter
				messageToPublish);
	}

	/**
	 * Convenience constructor: FREE service, no default message.
	 * Usually not used by CVM if createComponent cannot pass null.
	 */
	public MeteoStation(
			String receivingInboundURI,
			String brokerRegistrationInboundURI,
			String channel) throws Exception {
		super(
				receivingInboundURI,
				brokerRegistrationInboundURI,
				RegistrationClass.FREE,
				channel,
				null,
				null);
	}
}