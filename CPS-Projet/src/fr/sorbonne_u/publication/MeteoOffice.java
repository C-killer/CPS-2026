package fr.sorbonne_u.publication;

import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;

//另一个生产者角色,通常用于发布更高层级的消息,如天气预警(Alert)。
public class MeteoOffice extends Client {

	public MeteoOffice(String receivingInboundURI,
			String brokerRegistrationInboundURI,
			RegistrationClass serviceClass,
			String channel,
			MessageI messageToPublish) throws Exception {
		super(receivingInboundURI, brokerRegistrationInboundURI, serviceClass, channel, messageToPublish);
	}
}