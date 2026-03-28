package fr.sorbonne_u.publication.plugins;

import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.publication.Client;

//设定抽象类,ClientPublicationPlugin,ClientRegistrationPlugin.java,ClientSubscriptionPlugin继承之
/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public abstract class AbstractClientPlugin extends AbstractPlugin {
	private static final long serialVersionUID = 1L;

	protected String receivingInboundURI;
	protected String brokerRegistrationInboundURI;
	protected RegistrationClass serviceClass;

	public void configure(
			String receivingInboundURI,
			String brokerRegistrationInboundURI,
			RegistrationClass serviceClass) {
		this.receivingInboundURI = receivingInboundURI + "-port";
		this.brokerRegistrationInboundURI = brokerRegistrationInboundURI;
		this.serviceClass = serviceClass;
	}

	protected Client owner() {
		return (Client) this.getOwner();
	}
}