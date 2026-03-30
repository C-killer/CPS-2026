package fr.sorbonne_u.publication.components;

import fr.sorbonne_u.components.utils.tests.TestScenario;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.publication.Client;

/**
 * MeteoOffice = publisher client.
 * It typically publishes alert messages.
 * 
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class MeteoOffice extends Client {

	/** Constructor without scenario (for non-timed tests). */
	protected MeteoOffice(
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
				null,
				messageToPublish);
	}

	protected MeteoOffice(
			String receivingInboundURI,
			String brokerRegistrationInboundURI,
			RegistrationClass serviceClass,
			String channel,
			MessageI messageToPublish,
			TestScenario scenario) throws Exception {
		super(
				receivingInboundURI,
				brokerRegistrationInboundURI,
				serviceClass,
				channel,
				null, // no subscription filter
				messageToPublish,
				scenario);
	}
}