package fr.sorbonne_u.publication.plugins;

import java.time.Duration;
import java.util.concurrent.Future;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.messages.MessageFilter;
import fr.sorbonne_u.publication.plugins.interfaces.ClientSubscriptionI;
import fr.sorbonne_u.publication.ports.inbound.ReceivingInbound;

public class ClientSubscriptionPlugin
		extends AbstractClientPlugin
		implements ClientSubscriptionI {
	private static final long serialVersionUID = 1L;

	protected ReceivingInbound receivingInboundPort;

	@Override
	public void installOn(ComponentI owner) throws Exception {
		super.installOn(owner);

		this.addOfferedInterface(ReceivingCI.class);

		this.receivingInboundPort = new ReceivingInbound(this.receivingInboundURI, owner);
		this.receivingInboundPort.publishPort();
	}

	@Override
	public void initialise() throws Exception {
		super.initialise();
	}

	@Override
	public boolean channelExist(String channel) {
		return true;
	}

	@Override
	public boolean channelAuthorised(String channel) {
		return true;
	}

	@Override
	public boolean subscribed(String channel) throws Exception {
		return this.owner()
				.getRegistrationPlugin()
				.getRegistrationOutbound()
				.subscribed(this.receivingInboundURI, channel);
	}

	@Override
	public void subscribe(String channel, MessageFilterI filter) throws Exception {
		MessageFilterI f = (filter != null) ? filter : new MessageFilter(null, null, null);
		this.owner()
				.getRegistrationPlugin()
				.getRegistrationOutbound()
				.subscribe(this.receivingInboundURI, channel, f);
	}

	@Override
	public void unsubscribe(String channel) throws Exception {
		this.owner()
				.getRegistrationPlugin()
				.getRegistrationOutbound()
				.unsubscribe(this.receivingInboundURI, channel);
	}

	@Override
	public void modifyFilter(String channel, MessageFilterI filter) throws Exception {
		this.owner()
				.getRegistrationPlugin()
				.getRegistrationOutbound()
				.modifyFilter(this.receivingInboundURI, channel, filter);
	}

	@Override
	public void receive(String channel, MessageI message) {
		System.out.println("[" + this.receivingInboundURI + "] receive: channel="
				+ channel + ", message=" + message);
		this.owner().logMessage("RECEIVED on " + channel + " : " + message);
	}

	@Override
	public void receive(String channel, MessageI[] messages) {
		this.owner().logMessage("RECEIVED batch on " + channel + " : "
				+ (messages == null ? 0 : messages.length));
	}

	@Override
	public MessageI waitForNextMessage(String channel) {
		throw new UnsupportedOperationException("§3.5.3 not implemented yet");
	}

	@Override
	public MessageI waitForNextMessage(String channel, Duration d) {
		throw new UnsupportedOperationException("§3.5.3 not implemented yet");
	}

	@Override
	public Future<MessageI> getNextMessage(String channel) {
		throw new UnsupportedOperationException("§3.5.3 not implemented yet");
	}

	@Override
	public void uninstall() throws Exception {
		try {
			this.receivingInboundPort.unpublishPort();
		} catch (Exception ignored) {
		}
		this.removeOfferedInterface(ReceivingCI.class);
		super.uninstall();
	}
}