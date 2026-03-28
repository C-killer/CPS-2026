package fr.sorbonne_u.publication.ports.outbound;

import java.rmi.RemoteException;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.pubsub.exceptions.AlreadyRegisteredException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;

/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class RegistrationOutbound extends AbstractOutboundPort implements RegistrationCI {

	private static final long serialVersionUID = 1L;

	public RegistrationOutbound(ComponentI owner) throws Exception {
		super(RegistrationCI.class, owner);
	}

	public RegistrationOutbound(String uri, ComponentI owner) throws Exception {
		super(uri, RegistrationCI.class, owner);
	}

	@Override
	public boolean registered(String receptionPortURI) throws RemoteException {
		try {
			return ((RegistrationCI) this.getConnector()).registered(receptionPortURI);
		} catch (Exception e) {
			throw new RemoteException("Error: registered(receptionPortURI)", e);
		}
	}

	@Override
	public boolean registered(String receptionPortURI, RegistrationClass rc) throws RemoteException {
		try {
			return ((RegistrationCI) this.getConnector()).registered(receptionPortURI, rc);
		} catch (Exception e) {
			throw new RemoteException("Error: registered(receptionPortURI, rc)", e);
		}
	}

	@Override
	public String register(String receptionPortURI, RegistrationClass rc)
			throws RemoteException, AlreadyRegisteredException {
		try {
			return ((RegistrationCI) this.getConnector()).register(receptionPortURI, rc);
		} catch (AlreadyRegisteredException e) {
			throw e;
		} catch (Exception e) {
			throw new RemoteException("Error: register(receptionPortURI, rc)", e);
		}
	}

	@Override
	public String modifyServiceClass(String receptionPortURI, RegistrationClass rc) throws RemoteException {
		try {
			return ((RegistrationCI) this.getConnector()).modifyServiceClass(receptionPortURI, rc);
		} catch (Exception e) {
			throw new RemoteException("Error: modifyServiceClass(receptionPortURI, rc)", e);
		}
	}

	@Override
	public void unregister(String receptionPortURI) throws RemoteException {
		try {
			((RegistrationCI) this.getConnector()).unregister(receptionPortURI);
		} catch (Exception e) {
			throw new RemoteException("Error: unregister(receptionPortURI)", e);
		}
	}

	@Override
	public boolean channelExist(String channel) throws RemoteException {
		try {
			return ((RegistrationCI) this.getConnector()).channelExist(channel);
		} catch (Exception e) {
			throw new RemoteException("Error: channelExists(channel)", e);
		}
	}

	@Override
	public boolean channelAuthorised(String receptionPortURI, String channel) throws RemoteException {
		try {
			return ((RegistrationCI) this.getConnector()).channelAuthorised(receptionPortURI, channel);
		} catch (Exception e) {
			throw new RemoteException("Error: channelAuthorised(receptionPortURI, channel)", e);
		}
	}

	@Override
	public boolean subscribed(String receptionPortURI, String channel) throws RemoteException {
		try {
			return ((RegistrationCI) this.getConnector()).subscribed(receptionPortURI, channel);
		} catch (Exception e) {
			throw new RemoteException("Error: subscribed(receptionPortURI, channel)", e);
		}
	}

	@Override
	public void subscribe(String receptionPortURI, String channel, MessageFilterI filter) throws RemoteException {
		try {
			((RegistrationCI) this.getConnector()).subscribe(receptionPortURI, channel, filter);
		} catch (Exception e) {
			throw new RemoteException("Error: subscribe(receptionPortURI, channel, filter)", e);
		}
	}

	@Override
	public void unsubscribe(String receptionPortURI, String channel) throws RemoteException {
		try {
			((RegistrationCI) this.getConnector()).unsubscribe(receptionPortURI, channel);
		} catch (Exception e) {
			throw new RemoteException("Error: unsubscribe(receptionPortURI, channel)", e);
		}
	}

	@Override
	public boolean modifyFilter(String receptionPortURI, String channel, MessageFilterI filter) throws RemoteException {
		try {
			return ((RegistrationCI) this.getConnector()).modifyFilter(receptionPortURI, channel, filter);
		} catch (Exception e) {
			throw new RemoteException("Error: modifyFilter(receptionPortURI, channel, filter)", e);
		}
	}
}