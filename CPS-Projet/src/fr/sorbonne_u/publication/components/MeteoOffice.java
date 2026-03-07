package fr.sorbonne_u.publication.components;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.publication.Client;

/**
 * MeteoOffice = publisher client.
 * It typically publishes alert messages.
 */
public class MeteoOffice extends Client {

	public MeteoOffice(
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
}