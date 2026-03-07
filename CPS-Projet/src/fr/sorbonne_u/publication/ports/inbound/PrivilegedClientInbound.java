package fr.sorbonne_u.publication.ports.inbound;

import java.rmi.RemoteException;
import java.util.ArrayList;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownIdentifierException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;
import fr.sorbonne_u.publication.implementations.*;

//Semaine4
public class PrivilegedClientInbound extends AbstractInboundPort implements PrivilegedClientCI {
	private static final long serialVersionUID = 1L;

	public PrivilegedClientInbound(
			String uri,
			ComponentI owner) throws Exception {
		super(uri, PrivilegedClientCI.class, owner);
	}

	@Override
	public boolean channelExists(String channel)
			throws RemoteException {
		try {
			return ((PrivilegedClientCI) this.getOwner()).channelExists(channel);
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
			return ((PrivilegedClientCI) this.getOwner())
					.channelQuotaReached(receptionPortURI);
		} catch (RemoteException | UnknownIdentifierException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void createChannel(String receptionPortURI, String channel)
			throws RemoteException, UnknownIdentifierException {
		try {
			((PrivilegedClientCI) this.getOwner())
					.createChannel(receptionPortURI, channel);
		} catch (RemoteException | UnknownIdentifierException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void destroyChannel(String receptionPortURI, String channel)
			throws RemoteException, UnknownIdentifierException, UnknownChannelException {
		try {
			((PrivilegedClientCI) this.getOwner())
					.destroyChannel(receptionPortURI, channel);
		} catch (RemoteException | UnknownIdentifierException | UnknownChannelException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void destroyChannelNow(String receptionPortURI, String channel)
			throws RemoteException, UnknownIdentifierException, UnknownChannelException {
		try {
			((PrivilegedClientCI) this.getOwner())
					.destroyChannelNow(receptionPortURI, channel);
		} catch (RemoteException | UnknownIdentifierException | UnknownChannelException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void publish(String receptionPortURI, String channel, MessageI message)
			throws RemoteException, UnknownChannelException {
		try {
			((PublishingImplI) this.getOwner())
					.publish(receptionPortURI, channel, message);
		} catch (RemoteException | UnknownChannelException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void publish(String receptionPortURI, String channel, ArrayList<MessageI> messages)
			throws RemoteException, UnknownChannelException {
		try {
			((PublishingImplI) this.getOwner())
					.publish(receptionPortURI, channel, messages);
		} catch (RemoteException | UnknownChannelException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
