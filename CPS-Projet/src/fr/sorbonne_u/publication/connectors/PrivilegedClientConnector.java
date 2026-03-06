package fr.sorbonne_u.publication.connectors;

import java.util.ArrayList;

import java.rmi.RemoteException;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.pubsub.exceptions.AlreadyExistingChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownIdentifierException;
import fr.sorbonne_u.cps.pubsub.interfaces.ChannelQuotaExceededException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;

//semaine4
public class PrivilegedClientConnector extends AbstractConnector implements PrivilegedClientCI {
	@Override
	public boolean channelExists(String channel) throws RemoteException {
		return ((PrivilegedClientCI) this.offering).channelExists(channel);
	}

	@Override
	public boolean channelQuotaReached(String receptionPortURI) throws RemoteException, UnknownIdentifierException {
		return ((PrivilegedClientCI) this.offering).channelQuotaReached(receptionPortURI);
	}

	@Override
	public void createChannel(String receptionPortURI, String channel) throws RemoteException,
			UnknownIdentifierException, AlreadyExistingChannelException, ChannelQuotaExceededException {
		((PrivilegedClientCI) this.offering).createChannel(receptionPortURI, channel);
	}

	@Override
	public void destroyChannel(String receptionPortURI, String channel)
			throws RemoteException, UnknownIdentifierException, UnknownChannelException {
		((PrivilegedClientCI) this.offering).destroyChannel(receptionPortURI, channel);
	}

	@Override
	public void destroyChannelNow(String receptionPortURI, String channel)
			throws RemoteException, UnknownIdentifierException, UnknownChannelException {
		((PrivilegedClientCI) this.offering).destroyChannelNow(receptionPortURI, channel);
	}

	@Override
	public void publish(String receptionPortURI, String channel, MessageI message)
			throws RemoteException, UnknownChannelException {
		((PrivilegedClientCI) this.offering).publish(receptionPortURI, channel, message);
	}

	@Override
	public void publish(String receptionPortURI, String channel, ArrayList<MessageI> messages)
			throws RemoteException, UnknownChannelException {
		((PrivilegedClientCI) this.offering).publish(receptionPortURI, channel, messages);
	}

}
