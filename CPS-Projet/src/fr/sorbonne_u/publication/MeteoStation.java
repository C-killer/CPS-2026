package fr.sorbonne_u.publication;

import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;

public class MeteoStation extends Client {

	public MeteoStation(String receivingInboundURI,
			String brokerRegistrationInboundURI,
			RegistrationClass serviceClass,
			String channel,
			MessageI messageToPublish) throws Exception {
		super(receivingInboundURI, brokerRegistrationInboundURI, serviceClass, channel, messageToPublish);
	}

	// Convenience: FREE + default message in Client.defaultMessage()
	public MeteoStation(String receivingInboundURI,
			String brokerRegistrationInboundURI,
			String channel) throws Exception {
		super(receivingInboundURI, brokerRegistrationInboundURI, RegistrationClass.FREE, channel, (MessageI) null);
		// NOTE: we will NOT use this ctor in CVM because createComponent cannot pass
		// null.
	}
}