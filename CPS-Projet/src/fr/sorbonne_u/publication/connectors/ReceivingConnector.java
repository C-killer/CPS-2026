package fr.sorbonne_u.publication.connectors;

import java.rmi.RemoteException;

import fr.sorbonne_u.components.connectors.AbstractConnector;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;

public class ReceivingConnector extends AbstractConnector implements ReceivingCI {
	@Override
	public void receive(String channel, MessageI message) throws RemoteException {
		try {
			((ReceivingCI) this.offering).receive(channel, message);
		} catch (RemoteException e) {
			throw e;
		}
	}

	@Override
	public void receive(String channel, MessageI[] messages) throws RemoteException {
		try {
			((ReceivingCI) this.offering).receive(channel, messages);
		} catch (RemoteException e) {
			throw e;
		}
	}
}
