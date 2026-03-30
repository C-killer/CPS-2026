package fr.sorbonne_u.publication.ports.inbound;

import java.util.ArrayList;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;
import fr.sorbonne_u.publication.implementations.*;

//Semaine4
/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class PrivilegedClientInbound extends AbstractInboundPort implements PrivilegedClientCI {
	private static final long serialVersionUID = 1L;

	public PrivilegedClientInbound(
			String uri,
			ComponentI owner) throws Exception {
		super(uri, PrivilegedClientCI.class, owner);
	}

	@Override
	public boolean hasCreatedChannel(String receptionPortURI, String channel) throws Exception {
		return this.getOwner().handleRequest(
				o -> ((PrivilegedClientImpli) o).hasCreatedChannel(receptionPortURI, channel));
	}

	public boolean channelExist(String channel) throws Exception {
		return this.getOwner().handleRequest(
				o -> ((PrivilegedClientImpli) o).channelExist(channel));
	}

	@Override
	public boolean isAuthorisedUser(String channel, String uri) throws Exception {
		return this.getOwner().handleRequest(
				o -> ((PrivilegedClientImpli) o).isAuthorisedUser(channel, uri));
	}

	@Override
	public void modifyAuthorisedUsers(String receptionPortURI, String channel, String autorisedUsers) throws Exception {
		this.getOwner().handleRequest(o -> {
			((PrivilegedClientImpli) o).modifyAuthorisedUsers(receptionPortURI, channel, autorisedUsers);
			return null;
		});
	}

	@Override
	public void removeAuthorisedUsers(String receptionPortURI, String channel, String regularExpression)
			throws Exception {
		this.getOwner().handleRequest(o -> {
			((PrivilegedClientImpli) o).removeAuthorisedUsers(receptionPortURI, channel, regularExpression);
			return null;
		});
	}

	@Override
	public boolean channelQuotaReached(String receptionPortURI) throws Exception {
		return this.getOwner().handleRequest(
				o -> ((PrivilegedClientImpli) o).channelQuotaReached(receptionPortURI));
	}

	@Override
	public void createChannel(String receptionPortURI, String channel, String autorisedUsers) throws Exception {
		this.getOwner().handleRequest(o -> {
			((PrivilegedClientImpli) o).createChannel(receptionPortURI, channel, autorisedUsers);
			return null;
		});
	}

	@Override
	public void destroyChannel(String receptionPortURI, String channel) throws Exception {
		this.getOwner().handleRequest(o -> {
			((PrivilegedClientImpli) o).destroyChannel(receptionPortURI, channel);
			return null;
		});
	}

	@Override
	public void destroyChannelNow(String receptionPortURI, String channel) throws Exception {
		this.getOwner().handleRequest(o -> {
			((PrivilegedClientImpli) o).destroyChannelNow(receptionPortURI, channel);
			return null;
		});
	}

	@Override
	public void publish(String receptionPortURI, String channel, MessageI message)
			throws Exception {
		this.getOwner().runTask(o -> {
			try {
				((PrivilegedClientImpli) o).publish(receptionPortURI, channel, message);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
	}

	@Override
	public void publish(String receptionPortURI, String channel, ArrayList<MessageI> messages)
			throws Exception {
		this.getOwner().runTask(o -> {
			try {
				((PrivilegedClientImpli) o).publish(receptionPortURI, channel, messages);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
	}

	@Override
	public void asyncPublishAndNotify(String receptionPortURI, String channel, MessageI message,
			String notificationInboundPortURI) throws Exception {
		this.getOwner().runTask(o -> {
			try {
				((PrivilegedClientImpli) o).asyncPublishAndNotify(receptionPortURI, channel, message,
						notificationInboundPortURI);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
	}

	@Override
	public void asyncPublishAndNotify(String receptionPortURI, String channel, ArrayList<MessageI> messages,
			String notificationInboundPortURI) throws Exception {
		this.getOwner().runTask(o -> {
			try {
				((PrivilegedClientImpli) o).asyncPublishAndNotify(receptionPortURI, channel, messages,
						notificationInboundPortURI);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
	}
}
