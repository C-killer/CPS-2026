package fr.sorbonne_u.publication.components;

import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;

//代表系统中的生产者/发布者负责采集并向Broker发送气象数据(如风速)
//Acts as the producer/publisher in the system, responsible for collecting and sending meteorological data (such as wind speed) to the broker
/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class MeteoStation_before extends Client_before_plugins {

	protected MeteoStation_before(String receivingInboundURI,
			String brokerRegistrationInboundURI,
			RegistrationClass serviceClass,
			String channel,
			MessageI messageToPublish) throws Exception {
		super(receivingInboundURI, brokerRegistrationInboundURI, serviceClass, channel, messageToPublish);
	}

	// Convenience: FREE + default message in Client.defaultMessage()
	protected MeteoStation_before(String receivingInboundURI,
			String brokerRegistrationInboundURI,
			String channel) throws Exception {
		super(receivingInboundURI, brokerRegistrationInboundURI, RegistrationClass.FREE, channel, (MessageI) null);
		// NOTE: we will NOT use this ctor in CVM because createComponent cannot pass
		// null.
	}
}