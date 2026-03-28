package fr.sorbonne_u.publication.plugins;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.publication.connectors.RegistrationConnector;
import fr.sorbonne_u.publication.plugins.interfaces.ClientRegistrationI;
import fr.sorbonne_u.publication.ports.outbound.RegistrationOutbound;

/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class ClientRegistrationPlugin
		extends AbstractClientPlugin
		implements ClientRegistrationI {
	private static final long serialVersionUID = 1L;

	protected RegistrationOutbound registrationOutbound;
	protected String brokerPublishingInboundURI;

	@Override
	public void installOn(ComponentI owner) throws Exception {
		super.installOn(owner);

		this.addRequiredInterface(RegistrationCI.class);

		this.registrationOutbound = new RegistrationOutbound(owner);
		this.registrationOutbound.publishPort();
	}

	@Override
	public void initialise() throws Exception {
		super.initialise();

		this.registrationOutbound.doConnection(
				this.brokerRegistrationInboundURI,
				RegistrationConnector.class.getCanonicalName());
	}

	@Override
	public boolean registered() {
		try {
			return this.registrationOutbound.registered(this.receivingInboundURI);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean registered(RegistrationClass rc) throws Exception {
		return this.registrationOutbound.registered(this.receivingInboundURI);
	}

	@Override
	public void register(RegistrationClass rc) throws Exception {
		this.brokerPublishingInboundURI = this.registrationOutbound.register(this.receivingInboundURI, rc);
	}

	@Override
	public void modifyServiceClass(RegistrationClass rc) throws Exception {
		this.serviceClass = rc;
		this.brokerPublishingInboundURI = this.registrationOutbound.modifyServiceClass(this.receivingInboundURI, rc);
	}

	@Override
	public void unregister() throws Exception {
		this.registrationOutbound.unregister(this.receivingInboundURI);
	}

	public RegistrationOutbound getRegistrationOutbound() {
		return this.registrationOutbound;
	}

	public String getBrokerPublishingInboundURI() {
		return this.brokerPublishingInboundURI;
	}

	@Override
	public void finalise() throws Exception {
		try {
			if (this.registrationOutbound.connected()) {
				this.registrationOutbound.doDisconnection();
			}
		} catch (Exception ignored) {
		}
		super.finalise();
	}

	@Override
	public void uninstall() throws Exception {
		try {
			this.registrationOutbound.unpublishPort();
		} catch (Exception ignored) {
		}
		this.removeRequiredInterface(RegistrationCI.class);
		super.uninstall();
	}

}