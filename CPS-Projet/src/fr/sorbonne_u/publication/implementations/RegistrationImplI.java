package fr.sorbonne_u.publication.implementations;

import java.rmi.RemoteException;

import fr.sorbonne_u.cps.pubsub.exceptions.AlreadyRegisteredException;
import fr.sorbonne_u.cps.pubsub.exceptions.NotSubscribedChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownIdentifierException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;

public interface RegistrationImplI {
	boolean registered(String receptionPortURI) throws RemoteException;

	boolean registered(String receptionPortURI, RegistrationClass rc) throws RemoteException;

	String register(String receptionPortURI, RegistrationClass rc) throws RemoteException, AlreadyRegisteredException;

	String modifyServiceClass(String receptionPortURI, RegistrationClass rc)
			throws RemoteException, AlreadyRegisteredException;

	void unregister(String receptionPortURI) throws RemoteException, UnknownIdentifierException;

	boolean channelExists(String channel) throws RemoteException;

	boolean subscribed(String receptionPortURI, String channel) throws RemoteException, UnknownChannelException;

	void subscribe(String receptionPortURI, String channel, MessageFilterI filter)
			throws RemoteException, UnknownChannelException;

	void unsubscribe(String receptionPortURI, String channel)
			throws RemoteException, UnknownChannelException, NotSubscribedChannelException;

	boolean modifyFilter(String receptionPortURI, String channel, MessageFilterI filter)
			throws RemoteException, UnknownChannelException, NotSubscribedChannelException;
}
