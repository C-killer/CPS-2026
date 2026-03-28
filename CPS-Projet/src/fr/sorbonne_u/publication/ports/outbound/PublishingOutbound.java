package fr.sorbonne_u.publication.ports.outbound;

import java.rmi.RemoteException;
import java.util.ArrayList;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;

/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class PublishingOutbound extends AbstractOutboundPort implements PublishingCI {

	private static final long serialVersionUID = 1L;

	public PublishingOutbound(ComponentI owner) throws Exception {
		super(PublishingCI.class, owner);
	}

	public PublishingOutbound(String uri, ComponentI owner) throws Exception {
		super(uri, PublishingCI.class, owner);
	}

	@Override
	public void publish(String receptionPortURI, String channel, MessageI message)
			throws RemoteException, UnknownChannelException {
		try {
			((PublishingCI) this.getConnector())
					.publish(receptionPortURI, channel, message);
		} catch (UnknownChannelException e) {
			throw e;
		} catch (Exception e) {
			throw new RemoteException(
					"Error: publish(receptionPortURI, channel, message)", e);
		}
	}

	@Override
	public void publish(String receptionPortURI, String channel, ArrayList<MessageI> messages)
			throws RemoteException, UnknownChannelException {
		try {
			((PublishingCI) this.getConnector())
					.publish(receptionPortURI, channel, messages);
		} catch (UnknownChannelException e) {
			throw e;
		} catch (Exception e) {
			throw new RemoteException(
					"Error: publish(receptionPortURI, channel, messages)", e);
		}
	}

	@Override
	public void asyncPublishAndNotify(String receptionPortURI, String channel, MessageI message,
			String notificationInboundPortURI) throws Exception {
		try {
			((PublishingCI) this.getConnector())
					.asyncPublishAndNotify(receptionPortURI, channel, message, notificationInboundPortURI);
		} catch (Exception e) {
			throw new RemoteException(
					"Error: asyncPublishAndNotify(receptionPortURI, channel, message, notificationInboundPortURI)", e);
		}
	}

	@Override
	public void asyncPublishAndNotify(String receptionPortURI, String channel, ArrayList<MessageI> messages,
			String notificationInboundPortURI) throws Exception {
		try {
			((PublishingCI) this.getConnector())
					.asyncPublishAndNotify(receptionPortURI, channel, messages, notificationInboundPortURI);
		} catch (Exception e) {
			throw new RemoteException(
					"Error: asyncPublishAndNotify(receptionPortURI, channel, messages, notificationInboundPortURI)", e);
		}
	}
}