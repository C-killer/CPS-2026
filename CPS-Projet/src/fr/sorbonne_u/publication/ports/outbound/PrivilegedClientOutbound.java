package fr.sorbonne_u.publication.ports.outbound;

import java.rmi.RemoteException;
import java.util.ArrayList;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownIdentifierException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;

//Semaine 4
public class PrivilegedClientOutbound extends AbstractOutboundPort implements PrivilegedClientCI {
	private static final long serialVersionUID = 1L;

	public PrivilegedClientOutbound(ComponentI owner) throws Exception {
		super(PrivilegedClientCI.class, owner);
	}

	public PrivilegedClientOutbound(String uri, ComponentI owner) throws Exception {
		super(uri, PrivilegedClientCI.class, owner);
	}

	@Override
	public boolean channelExists(String channel)
			throws RemoteException {
		try {
			return ((PrivilegedClientCI) this.getConnector()).channelExists(channel);
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
			return ((PrivilegedClientCI) this.getConnector())
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
			((PrivilegedClientCI) this.getConnector())
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
			((PrivilegedClientCI) this.getConnector())
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
			((PrivilegedClientCI) this.getConnector())
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
			((PrivilegedClientCI) this.getConnector())
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
			((PrivilegedClientCI) this.getConnector())
					.publish(receptionPortURI, channel, messages);
		} catch (RemoteException | UnknownChannelException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
