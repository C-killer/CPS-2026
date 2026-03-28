package fr.sorbonne_u.publication.ports.inbound;

import java.rmi.RemoteException;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.pubsub.exceptions.AlreadyRegisteredException;
import fr.sorbonne_u.cps.pubsub.exceptions.NotSubscribedChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownIdentifierException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.publication.implementations.RegistrationImplI;

/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class RegistrationInbound extends AbstractInboundPort implements RegistrationCI {

	private static final long serialVersionUID = 1L;

	public RegistrationInbound(ComponentI owner) throws Exception {
		super(RegistrationCI.class, owner);
		assert owner instanceof RegistrationImplI
				: "Owner must implement RegistrationImplI to use RegistrationInbound.";
	}

	public RegistrationInbound(String uri, ComponentI owner) throws Exception {
		super(uri, RegistrationCI.class, owner);
		assert owner instanceof RegistrationImplI
				: "Owner must implement RegistrationImplI to use RegistrationInbound.";
	}

	@Override
	public boolean registered(String receptionPortURI) throws RemoteException {
		try {
			return this.getOwner().handleRequest(o -> ((RegistrationImplI) o).registered(receptionPortURI));
		} catch (Exception e) {
			throw new RemoteException("Cannot schedule: registered(receptionPortURI)", e);
		}
	}

	@Override
	public boolean registered(String receptionPortURI, RegistrationClass rc) throws RemoteException {
		try {
			return this.getOwner().handleRequest(o -> ((RegistrationImplI) o).registered(receptionPortURI, rc));
		} catch (Exception e) {
			throw new RemoteException("Cannot schedule: registered(receptionPortURI, rc)", e);
		}
	}

	@Override
	public String register(String receptionPortURI, RegistrationClass rc)
			throws RemoteException, AlreadyRegisteredException {
		try {
			return this.getOwner().handleRequest(o -> ((RegistrationImplI) o).register(receptionPortURI, rc));
		} catch (AlreadyRegisteredException e) {
			throw e;
		} catch (Exception e) {
			throw new RemoteException("Cannot schedule: register(receptionPortURI, rc)", e);
		}
	}

	@Override
	public String modifyServiceClass(String receptionPortURI, RegistrationClass rc)
			throws RemoteException, AlreadyRegisteredException {
		try {
			return this.getOwner().handleRequest(o -> ((RegistrationImplI) o).modifyServiceClass(receptionPortURI, rc));
		} catch (AlreadyRegisteredException e) {
			throw e;
		} catch (Exception e) {
			throw new RemoteException("Cannot schedule: modifyServiceClass(receptionPortURI, rc)", e);
		}
	}

	@Override
	public void unregister(String receptionPortURI) throws RemoteException, UnknownIdentifierException {
		try {
			this.getOwner().handleRequest(o -> {
				((RegistrationImplI) o).unregister(receptionPortURI);
				return null;
			});
		} catch (UnknownIdentifierException e) {
			throw e;
		} catch (Exception e) {
			throw new RemoteException("Cannot schedule: unregister(receptionPortURI)", e);
		}
	}

	@Override
	public boolean channelExist(String channel) throws RemoteException {
		try {
			return this.getOwner().handleRequest(o -> ((RegistrationImplI) o).channelExist(channel));
		} catch (Exception e) {
			throw new RemoteException("Cannot schedule: channelExists(channel)", e);
		}
	}

	@Override
	public boolean channelAuthorised(String receptionPortURI, String channel) throws RemoteException {
		try {
			return this.getOwner()
					.handleRequest(o -> ((RegistrationImplI) o).channelAuthorised(receptionPortURI, channel));
		} catch (Exception e) {
			throw new RemoteException("Cannot schedule: channelAuthorised(receptionPortURI, channel)", e);
		}
	}

	@Override
	public boolean subscribed(String receptionPortURI, String channel) throws RemoteException, UnknownChannelException {
		try {
			return this.getOwner().handleRequest(o -> ((RegistrationImplI) o).subscribed(receptionPortURI, channel));
		} catch (UnknownChannelException e) {
			throw e;
		} catch (Exception e) {
			throw new RemoteException("Cannot schedule: subscribed(receptionPortURI, channel)", e);
		}
	}

	@Override
	public void subscribe(String receptionPortURI, String channel, MessageFilterI filter)
			throws RemoteException, UnknownChannelException {
		try {
			this.getOwner().handleRequest(o -> {
				((RegistrationImplI) o).subscribe(receptionPortURI, channel, filter);
				return null;
			});
		} catch (UnknownChannelException e) {
			throw e;
		} catch (Exception e) {
			throw new RemoteException("Cannot schedule: subscribe(receptionPortURI, channel, filter)", e);
		}
	}

	@Override
	public void unsubscribe(String receptionPortURI, String channel)
			throws RemoteException, UnknownChannelException, NotSubscribedChannelException {
		try {
			this.getOwner().handleRequest(o -> {
				((RegistrationImplI) o).unsubscribe(receptionPortURI, channel);
				return null;
			});
		} catch (UnknownChannelException e) {
			throw e;
		} catch (NotSubscribedChannelException e) {
			throw e;
		} catch (Exception e) {
			throw new RemoteException("Cannot schedule: unsubscribe(receptionPortURI, channel)", e);
		}
	}

	@Override
	public boolean modifyFilter(String receptionPortURI, String channel, MessageFilterI filter)
			throws RemoteException, UnknownChannelException, NotSubscribedChannelException {
		try {
			return this.getOwner()
					.handleRequest(o -> ((RegistrationImplI) o).modifyFilter(receptionPortURI, channel, filter));
		} catch (UnknownChannelException e) {
			throw e;
		} catch (NotSubscribedChannelException e) {
			throw e;
		} catch (Exception e) {
			throw new RemoteException("Cannot schedule: modifyFilter(receptionPortURI, channel, filter)", e);
		}
	}
}