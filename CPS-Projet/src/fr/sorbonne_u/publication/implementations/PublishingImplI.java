package fr.sorbonne_u.publication.implementations;

import java.rmi.RemoteException;
import java.util.ArrayList;

import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;

public interface PublishingImplI {
	void publish(String receptionPortURI, String channel, MessageI message)
			throws RemoteException, UnknownChannelException;

	void publish(String receptionPortURI, String channel, ArrayList<MessageI> messages)
			throws RemoteException, UnknownChannelException;
}
