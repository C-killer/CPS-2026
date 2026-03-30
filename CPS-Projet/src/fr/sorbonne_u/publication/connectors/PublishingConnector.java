package fr.sorbonne_u.publication.connectors;

import java.rmi.RemoteException;
import java.util.ArrayList;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;

/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class PublishingConnector extends AbstractConnector implements PublishingCI {
	@Override
	public void publish(String receptionPortURI, String channel, MessageI message)
			throws RemoteException, UnknownChannelException {
		try {
			((PublishingCI) this.offering)
					.publish(receptionPortURI, channel, message);
		} catch (UnknownChannelException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RemoteException(
					"Error: Connector publish(receptionPortURI, channel, message)", e);
		}
	}

	@Override
	public void publish(String receptionPortURI, String channel, ArrayList<MessageI> messages)
			throws RemoteException, UnknownChannelException {
		try {
			((PublishingCI) this.offering)
					.publish(receptionPortURI, channel, messages);
		} catch (UnknownChannelException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RemoteException(
					"Error: Connector publish(receptionPortURI, channel, messages)", e);
		}
	}

	@Override
	public void asyncPublishAndNotify(String receptionPortURI, String channel, MessageI message,
			String notificationInboundPortURI) throws Exception {
		try {
			((PublishingCI) this.offering).asyncPublishAndNotify(receptionPortURI, channel, message,
					notificationInboundPortURI);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RemoteException("Error: Connector asyncPublishAndNotify", e);
		}
	}

	@Override
	public void asyncPublishAndNotify(String receptionPortURI, String channel, ArrayList<MessageI> messages,
			String notificationInboundPortURI) throws Exception {
		try {
			((PublishingCI) this.offering).asyncPublishAndNotify(receptionPortURI, channel, messages,
					notificationInboundPortURI);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RemoteException("Error: Connector asyncPublishAndNotify", e);
		}
	}
}