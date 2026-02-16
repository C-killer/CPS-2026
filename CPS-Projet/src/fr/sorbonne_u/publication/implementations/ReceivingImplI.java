package fr.sorbonne_u.publication.implementations;

import java.rmi.RemoteException;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;

public interface ReceivingImplI {
	void receive(String channel, MessageI message) throws RemoteException;

	void receive(String channel, MessageI[] messages) throws RemoteException;
}
