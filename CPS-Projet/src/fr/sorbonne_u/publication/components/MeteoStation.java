package fr.sorbonne_u.publication.components;

import fr.sorbonne_u.components.utils.tests.TestScenario;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.publication.Client;

/**
 * MeteoStation = publisher client.
 * It publishes one weather message to a channel.
 * 
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class MeteoStation extends Client {

	protected MeteoStation(
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

	/** Constructor without scenario (for non-timed tests). */
	protected MeteoStation(
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

	/**
	 * Convenience constructor: FREE service, no default message.
	 * Usually not used by CVM if createComponent cannot pass null.
	 */
	protected MeteoStation(
			String receivingInboundURI,
			String brokerRegistrationInboundURI,
			String channel,
			TestScenario scenario) throws Exception {
		super(
				receivingInboundURI,
				brokerRegistrationInboundURI,
				RegistrationClass.FREE,
				channel,
				null,
				null,
				scenario);
	}
}