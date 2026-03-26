package fr.sorbonne_u.publication.connectors;

import java.util.ArrayList;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.publication.implementations.PrivilegedClientImpli;

//semaine4
public class PrivilegedClientConnector extends AbstractConnector implements PrivilegedClientImpli {

	@Override
	public boolean hasCreatedChannel(String receptionPortURI, String channel) throws Exception {
		return ((PrivilegedClientImpli) this.offering).hasCreatedChannel(receptionPortURI, channel);
	}

	@Override
	public boolean channelExist(String channel) throws Exception {
		return ((PrivilegedClientImpli) this.offering).channelExist(channel);
	}

	@Override
	public boolean isAuthorisedUser(String channel, String uri) throws Exception {
		return ((PrivilegedClientImpli) this.offering).isAuthorisedUser(channel, uri);
	}

	@Override
	public void modifyAuthorisedUsers(String receptionPortURI, String channel, String autorisedUsers) throws Exception {
		((PrivilegedClientImpli) this.offering).modifyAuthorisedUsers(receptionPortURI, channel, autorisedUsers);
	}

	@Override
	public void removeAuthorisedUsers(String receptionPortURI, String channel, String regularExpression)
			throws Exception {
		((PrivilegedClientImpli) this.offering).removeAuthorisedUsers(receptionPortURI, channel, regularExpression);
	}

	@Override
	public boolean channelQuotaReached(String receptionPortURI) throws Exception {
		return ((PrivilegedClientImpli) this.offering).channelQuotaReached(receptionPortURI);
	}

	@Override
	public void createChannel(String receptionPortURI, String channel, String autorisedUsers) throws Exception {
		((PrivilegedClientImpli) this.offering).createChannel(receptionPortURI, channel, autorisedUsers);
	}

	@Override
	public void destroyChannel(String receptionPortURI, String channel)
			throws Exception {
		((PrivilegedClientImpli) this.offering).destroyChannel(receptionPortURI, channel);
	}

	@Override
	public void destroyChannelNow(String receptionPortURI, String channel)
			throws Exception {
		((PrivilegedClientImpli) this.offering).destroyChannelNow(receptionPortURI, channel);
	}

	@Override
	public void publish(String receptionPortURI, String channel, MessageI message)
			throws Exception {
		((PrivilegedClientImpli) this.offering).publish(receptionPortURI, channel, message);
	}

	@Override
	public void publish(String receptionPortURI, String channel, ArrayList<MessageI> messages)
			throws Exception {
		((PrivilegedClientImpli) this.offering).publish(receptionPortURI, channel, messages);
	}

	@Override
	public void asyncPublishAndNotify(String receptionPortURI, String channel, MessageI message,
			String notificationInboundPortURI) throws Exception {
		((PrivilegedClientImpli) this.offering).asyncPublishAndNotify(receptionPortURI, channel, message,
				notificationInboundPortURI);
	}

	@Override
	public void asyncPublishAndNotify(String receptionPortURI, String channel, ArrayList<MessageI> messages,
			String notificationInboundPortURI) throws Exception {
		((PrivilegedClientImpli) this.offering).asyncPublishAndNotify(receptionPortURI, channel, messages,
				notificationInboundPortURI);
	}

}
