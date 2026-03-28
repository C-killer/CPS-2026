package fr.sorbonne_u.publication.ports.inbound;

import java.rmi.RemoteException;
import java.util.ArrayList;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;
import fr.sorbonne_u.publication.implementations.*;

//Semaine4
public class PrivilegedClientInbound extends AbstractInboundPort implements PrivilegedClientCI {
	private static final long serialVersionUID = 1L;

	public PrivilegedClientInbound(
			String uri,
			ComponentI owner) throws Exception {
		super(uri, PrivilegedClientCI.class, owner);
	}

	@Override
	public boolean hasCreatedChannel(String receptionPortURI, String channel) throws Exception {
		try {
			return ((PrivilegedClientImpli) this.getOwner()).hasCreatedChannel(receptionPortURI, channel);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public boolean channelExist(String channel) throws Exception {
		return ((PrivilegedClientImpli) this.getOwner()).channelExist(channel);
	}

	@Override
	public boolean isAuthorisedUser(String channel, String uri) throws Exception {
		return ((PrivilegedClientImpli) this.getOwner()).isAuthorisedUser(channel, uri);
	}

	@Override
	public void modifyAuthorisedUsers(String receptionPortURI, String channel, String autorisedUsers) throws Exception {
		((PrivilegedClientImpli) this.getOwner()).modifyAuthorisedUsers(receptionPortURI, channel, autorisedUsers);
	}

	@Override
	public void removeAuthorisedUsers(String receptionPortURI, String channel, String regularExpression)
			throws Exception {
		((PrivilegedClientImpli) this.getOwner()).removeAuthorisedUsers(receptionPortURI, channel, regularExpression);
	}

	@Override
	public boolean channelQuotaReached(String receptionPortURI) throws Exception {
		return ((PrivilegedClientImpli) this.getOwner()).channelQuotaReached(receptionPortURI);
	}

	@Override
	public void createChannel(String receptionPortURI, String channel, String autorisedUsers) throws Exception {
		((PrivilegedClientImpli) this.getOwner()).createChannel(receptionPortURI, channel, autorisedUsers);
	}

	@Override
	public void destroyChannel(String receptionPortURI, String channel) throws Exception {
		((PrivilegedClientImpli) this.getOwner()).destroyChannel(receptionPortURI, channel);
	}

	@Override
	public void destroyChannelNow(String receptionPortURI, String channel) throws Exception {
		((PrivilegedClientImpli) this.getOwner()).destroyChannelNow(receptionPortURI, channel);
	}

	@Override
	public void publish(String receptionPortURI, String channel, MessageI message)
			throws Exception {
		try {
			((PrivilegedClientImpli) this.getOwner())
					.publish(receptionPortURI, channel, message);
		} catch (RemoteException | UnknownChannelException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void publish(String receptionPortURI, String channel, ArrayList<MessageI> messages)
			throws Exception {
		try {
			((PrivilegedClientImpli) this.getOwner())
					.publish(receptionPortURI, channel, messages);
		} catch (RemoteException | UnknownChannelException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void asyncPublishAndNotify(String receptionPortURI, String channel, MessageI message,
			String notificationInboundPortURI) throws Exception {
		try {
			((PrivilegedClientImpli) this.getOwner()).asyncPublishAndNotify(receptionPortURI, channel, message,
					notificationInboundPortURI);
		} catch (Exception e) {
			throw new RemoteException("Error: asyncPublishAndNotify", e);
		}
	}

	@Override
	public void asyncPublishAndNotify(String receptionPortURI, String channel, ArrayList<MessageI> messages,
			String notificationInboundPortURI) throws Exception {
		try {
			((PrivilegedClientImpli) this.getOwner()).asyncPublishAndNotify(receptionPortURI, channel, messages,
					notificationInboundPortURI);
		} catch (Exception e) {
			throw new RemoteException("Error: asyncPublishAndNotify", e);
		}
	}
}
