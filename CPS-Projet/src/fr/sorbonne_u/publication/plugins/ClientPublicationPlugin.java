package fr.sorbonne_u.publication.plugins;

import java.util.ArrayList;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.publication.connectors.PrivilegedClientConnector;
import fr.sorbonne_u.publication.connectors.PublishingConnector;
import fr.sorbonne_u.publication.interfaces.AbnormalTerminationNotificationCI;
import fr.sorbonne_u.publication.plugins.interfaces.ClientPublicationI;
import fr.sorbonne_u.publication.ports.inbound.AbnormalTerminationNotificationInbound;
import fr.sorbonne_u.publication.ports.outbound.PrivilegedClientOutbound;
import fr.sorbonne_u.publication.ports.outbound.PublishingOutbound;

//使Client的操作分化到该插件,发布插件能实现从发布到销毁端口相关周期操作
/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class ClientPublicationPlugin
		extends AbstractClientPlugin
		implements ClientPublicationI {
	private static final long serialVersionUID = 1L;

	protected PublishingOutbound publishingOutbound;
	protected PrivilegedClientOutbound privilegedPublishingOutbound;
	protected AbnormalTerminationNotificationInbound notificationInbound;
	protected String brokerPublishingInboundURI;
	protected String notificationInboundURI;

	@Override
	public void installOn(ComponentI owner) throws Exception {
		super.installOn(owner);

		this.addRequiredInterface(PublishingCI.class);
		this.addRequiredInterface(PrivilegedClientCI.class);
		this.addOfferedInterface(AbnormalTerminationNotificationCI.class);

		this.publishingOutbound = new PublishingOutbound(owner);
		this.publishingOutbound.publishPort();

		this.privilegedPublishingOutbound = new PrivilegedClientOutbound(owner);
		this.privilegedPublishingOutbound.publishPort();

		this.notificationInboundURI = this.receivingInboundURI + "-notification";
		this.notificationInbound = new AbnormalTerminationNotificationInbound(
				this.notificationInboundURI, owner);
		this.notificationInbound.publishPort();
	}

	@Override
	public void initialise() throws Exception {
		super.initialise();
	}

	public void connectToBroker(String brokerPublishingInboundURI) throws Exception {
		this.brokerPublishingInboundURI = brokerPublishingInboundURI;

		if (this.serviceClass == RegistrationClass.FREE) {
			this.publishingOutbound.doConnection(
					this.brokerPublishingInboundURI,
					PublishingConnector.class.getCanonicalName());
		} else {
			this.privilegedPublishingOutbound.doConnection(
					this.brokerPublishingInboundURI,
					PrivilegedClientConnector.class.getCanonicalName());
		}
	}

	@Override
	public boolean channelExist(String channel) {
		try {
			return this.owner().getRegistrationPlugin().getRegistrationOutbound().channelExist(channel);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean channelAuthorised(String channel) throws Exception {
		return this.owner().getRegistrationPlugin().getRegistrationOutbound()
				.channelAuthorised(this.receivingInboundURI, channel);
	}

	@Override
	public void publish(String channel, MessageI message) throws Exception {
		if (this.serviceClass == RegistrationClass.FREE) {
			this.publishingOutbound.publish(this.receivingInboundURI, channel, message);
		} else {
			this.privilegedPublishingOutbound.publish(this.receivingInboundURI, channel, message);
		}
	}

	@Override
	public void publish(String channel, ArrayList<MessageI> messages) throws Exception {
		if (this.serviceClass == RegistrationClass.FREE) {
			this.publishingOutbound.publish(this.receivingInboundURI, channel, messages);
		} else {
			this.privilegedPublishingOutbound.publish(this.receivingInboundURI, channel, messages);
		}
	}

	public MessageI defaultMessage() throws Exception {
		Message m = new Message(null);
		m.setPayload("demo");
		m.putProperty("type", "wind");
		m.putProperty("speed", 42);
		return m;
	}

	public void createChannel(String channel, String autorisedUsers) throws Exception {
		if (this.serviceClass == RegistrationClass.FREE) {
			throw new IllegalStateException("FREE client cannot create channels");
		}
		this.privilegedPublishingOutbound.createChannel(this.receivingInboundURI, channel, autorisedUsers);
	}

	public void destroyChannel(String channel) throws Exception {
		if (this.serviceClass == RegistrationClass.FREE) {
			throw new IllegalStateException("FREE client cannot destroy channels");
		}
		this.privilegedPublishingOutbound.destroyChannel(this.receivingInboundURI, channel);
	}

	public void destroyChannelNow(String channel) throws Exception {
		if (this.serviceClass == RegistrationClass.FREE) {
			throw new IllegalStateException("FREE client cannot destroy channels");
		}
		this.privilegedPublishingOutbound.destroyChannelNow(this.receivingInboundURI, channel);
	}

	public boolean channelQuotaReached() throws Exception {
		if (this.serviceClass == RegistrationClass.FREE) {
			return true;
		}
		return this.privilegedPublishingOutbound.channelQuotaReached(this.receivingInboundURI);
	}

	// Operations de privileged
	public boolean hasCreatedChannel(String channel) throws Exception {
		if (this.serviceClass == RegistrationClass.FREE) {
			return false;
		}
		return this.privilegedPublishingOutbound.hasCreatedChannel(this.receivingInboundURI, channel);
	}

	public boolean isAuthorisedUser(String channel, String uri) throws Exception {
		if (this.serviceClass == RegistrationClass.FREE) {
			return false;
		}
		return this.privilegedPublishingOutbound.isAuthorisedUser(channel, uri);
	}

	public void modifyAuthorisedUsers(String channel, String autorisedUsers) throws Exception {
		if (this.serviceClass == RegistrationClass.FREE) {
			throw new IllegalStateException("FREE client cannot modify channel authorisations");
		}
		this.privilegedPublishingOutbound.modifyAuthorisedUsers(this.receivingInboundURI, channel, autorisedUsers);
	}

	public void removeAuthorisedUsers(String channel, String regularExpression) throws Exception {
		if (this.serviceClass == RegistrationClass.FREE) {
			throw new IllegalStateException("FREE client cannot remove channel authorisations");
		}
		this.privilegedPublishingOutbound.removeAuthorisedUsers(this.receivingInboundURI, channel, regularExpression);
	}

	// End de operations de privileged
	@Override
	public void finalise() throws Exception {
		try {
			if (this.publishingOutbound.connected()) {
				this.publishingOutbound.doDisconnection();
			}
		} catch (Exception ignored) {
		}
		try {
			if (this.privilegedPublishingOutbound.connected()) {
				this.privilegedPublishingOutbound.doDisconnection();
			}
		} catch (Exception ignored) {
		}
		super.finalise();
	}

	@Override
	public void uninstall() throws Exception {
		try { this.publishingOutbound.unpublishPort(); }              catch (Exception ignored) {}
		try { this.privilegedPublishingOutbound.unpublishPort(); }    catch (Exception ignored) {}
		try { this.notificationInbound.unpublishPort(); }             catch (Exception ignored) {}

		this.removeRequiredInterface(PrivilegedClientCI.class);
		this.removeRequiredInterface(PublishingCI.class);
		this.removeOfferedInterface(AbnormalTerminationNotificationCI.class);

		super.uninstall();
	}

	@Override
	public void asyncPublishAndNotify(String channel, MessageI message) throws Exception {
		String notificationURI = this.receivingInboundURI + "-notification";

		if (this.serviceClass == RegistrationClass.FREE) {
			this.publishingOutbound.asyncPublishAndNotify(
					this.receivingInboundURI, channel, message, notificationURI);
		} else {
			this.privilegedPublishingOutbound.asyncPublishAndNotify(
					this.receivingInboundURI, channel, message, notificationURI);
		}
	}

	@Override
	public void asyncPublishAndNotify(String channel, ArrayList<MessageI> messages) throws Exception {
		String notificationURI = this.receivingInboundURI + "-notification";

		if (this.serviceClass == RegistrationClass.FREE) {
			this.publishingOutbound.asyncPublishAndNotify(
					this.receivingInboundURI, channel, messages, notificationURI);
		} else {
			this.privilegedPublishingOutbound.asyncPublishAndNotify(
					this.receivingInboundURI, channel, messages, notificationURI);
		}
	}
}