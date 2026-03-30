package fr.sorbonne_u.publication.components;

import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;

//另一个生产者角色,通常用于发布更高层级的消息,如天气预警(Alert)。
//A producer role, typically used to publish higher-level messages, such as weather alerts
/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class MeteoOffice_before extends Client_before_plugins {

	protected MeteoOffice_before(String receivingInboundURI,
			String brokerRegistrationInboundURI,
			RegistrationClass serviceClass,
			String channel,
			MessageI messageToPublish) throws Exception {
		super(receivingInboundURI, brokerRegistrationInboundURI, serviceClass, channel, messageToPublish);
	}
}