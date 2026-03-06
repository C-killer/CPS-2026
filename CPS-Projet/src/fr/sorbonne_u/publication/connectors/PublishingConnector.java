package fr.sorbonne_u.publication.connectors;

import java.rmi.RemoteException;
import java.util.ArrayList;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;

public class PublishingConnector extends AbstractConnector implements PublishingCI {
	// 两种实现方式实现灵活调用,对单条消息直接用第一种,多条消息不用重复使用第一种
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
}