package fr.sorbonne_u.publication.ports.outbound;

import java.rmi.RemoteException;
import java.util.ArrayList;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.publication.connectors.PrivilegedClientConnector;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownIdentifierException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;

//Semaine 4
/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class PrivilegedClientOutbound extends AbstractOutboundPort implements PrivilegedClientCI {
	private static final long serialVersionUID = 1L;

	public PrivilegedClientOutbound(ComponentI owner) throws Exception {
		super(PrivilegedClientCI.class, owner);
	}

	public PrivilegedClientOutbound(String uri, ComponentI owner) throws Exception {
		super(uri, PrivilegedClientCI.class, owner);
	}

	@Override
	public boolean hasCreatedChannel(String receptionPortURI, String channel) throws Exception {
		try {
			return ((PrivilegedClientCI) this.getConnector()).hasCreatedChannel(receptionPortURI, channel);
		} catch (Exception e) {
			throw new RemoteException("Error: hasCreatedChannel", e);
		}
	}

	@SuppressWarnings("deprecation")
	@Override
	public boolean isAuthorisedUser(String channel, String uri) throws Exception {
		try {
			return ((PrivilegedClientCI) this.getConnector()).isAuthorisedUser(channel, uri);
		} catch (Exception e) {
			throw new RemoteException("Error: isAuthorisedUser", e);
		}
	}

	@Override
	public void modifyAuthorisedUsers(String receptionPortURI, String channel, String autorisedUsers) throws Exception {
		try {
			((PrivilegedClientCI) this.getConnector()).modifyAuthorisedUsers(receptionPortURI, channel,
					autorisedUsers);
		} catch (Exception e) {
			throw new RemoteException("Error: modifyAuthorisedUsers", e);
		}
	}

	@SuppressWarnings("deprecation")
	@Override
	public void removeAuthorisedUsers(String receptionPortURI, String channel, String regularExpression)
			throws Exception {
		try {
			((PrivilegedClientCI) this.getConnector()).removeAuthorisedUsers(receptionPortURI, channel,
					regularExpression);
		} catch (Exception e) {
			throw new RemoteException("Error: removeAuthorisedUsers", e);
		}
	}

	public boolean channelExist(String channel)
			throws RemoteException {
		try {
			return ((PrivilegedClientConnector) this.getConnector()).channelExist(channel);
		} catch (RemoteException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean channelQuotaReached(String receptionPortURI)
			throws RemoteException, UnknownIdentifierException {
		try {
			return ((PrivilegedClientCI) this.getConnector())
					.channelQuotaReached(receptionPortURI);
		} catch (RemoteException | UnknownIdentifierException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void createChannel(String receptionPortURI, String channel, String autorisedUsers) throws Exception {
		try {
			((PrivilegedClientCI) this.getConnector()).createChannel(receptionPortURI, channel, autorisedUsers);
		} catch (Exception e) {
			throw new RemoteException("Error: createChannel", e);
		}
	}

	@Override
	public void destroyChannel(String receptionPortURI, String channel) throws Exception {
		try {
			((PrivilegedClientCI) this.getConnector()).destroyChannel(receptionPortURI, channel);
		} catch (Exception e) {
			throw new RemoteException("Error: destroyChannel", e);
		}
	}

	@Override
	public void destroyChannelNow(String receptionPortURI, String channel) throws Exception {
		try {
			((PrivilegedClientCI) this.getConnector()).destroyChannelNow(receptionPortURI, channel);
		} catch (Exception e) {
			throw new RemoteException("Error: destroyChannelNow", e);
		}
	}

	@Override
	public void publish(String receptionPortURI, String channel, MessageI message) throws Exception {
		try {
			((PrivilegedClientCI) this.getConnector()).publish(receptionPortURI, channel, message);
		} catch (Exception e) {
			throw new RemoteException("Error: publish", e);
		}
	}

	@Override
	public void publish(String receptionPortURI, String channel, ArrayList<MessageI> messages) throws Exception {
		try {
			((PrivilegedClientCI) this.getConnector()).publish(receptionPortURI, channel, messages);
		} catch (Exception e) {
			throw new RemoteException("Error: publish", e);
		}
	}

	@Override
	public void asyncPublishAndNotify(String receptionPortURI, String channel, MessageI message,
			String notificationInboundPortURI) throws Exception {
		try {
			((PrivilegedClientCI) this.getConnector()).asyncPublishAndNotify(receptionPortURI, channel, message,
					notificationInboundPortURI);
		} catch (Exception e) {
			throw new RemoteException("Error: asyncPublishAndNotify", e);
		}
	}

	@Override
	public void asyncPublishAndNotify(String receptionPortURI, String channel, ArrayList<MessageI> messages,
			String notificationInboundPortURI) throws Exception {
		try {
			((PrivilegedClientCI) this.getConnector()).asyncPublishAndNotify(receptionPortURI, channel, messages,
					notificationInboundPortURI);
		} catch (Exception e) {
			throw new RemoteException("Error: asyncPublishAndNotify", e);
		}
	}
}
