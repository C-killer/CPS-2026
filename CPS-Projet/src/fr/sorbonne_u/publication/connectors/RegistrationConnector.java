package fr.sorbonne_u.publication.connectors;

import java.rmi.RemoteException;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.pubsub.exceptions.AlreadyRegisteredException;
import fr.sorbonne_u.cps.pubsub.exceptions.NotSubscribedChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownIdentifierException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;

/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class RegistrationConnector extends AbstractConnector implements RegistrationCI {

	@Override
	public boolean registered(String receptionPortURI) throws RemoteException {
		try {
			return ((RegistrationCI) this.offering).registered(receptionPortURI);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RemoteException("Error: Connector registered(receptionPortURI)", e);
		}
	}

	@Override
	public boolean registered(String receptionPortURI, RegistrationClass rc) throws RemoteException {
		try {
			return ((RegistrationCI) this.offering).registered(receptionPortURI, rc);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RemoteException("Error: Connector registered(receptionPortURI, rc)", e);
		}
	}

	@Override
	public String register(String receptionPortURI, RegistrationClass rc)
			throws RemoteException, AlreadyRegisteredException {
		try {
			return ((RegistrationCI) this.offering).register(receptionPortURI, rc);
		} catch (AlreadyRegisteredException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RemoteException("Error: Connector register(receptionPortURI, rc)", e);
		}
	}

	@Override
	public String modifyServiceClass(String receptionPortURI, RegistrationClass rc)
			throws RemoteException, AlreadyRegisteredException {
		try {
			return ((RegistrationCI) this.offering).modifyServiceClass(receptionPortURI, rc);
		} catch (AlreadyRegisteredException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RemoteException("Error: Connector modifyServiceClass(receptionPortURI, rc)", e);
		}
	}

	@Override
	public void unregister(String receptionPortURI) throws RemoteException, UnknownIdentifierException {
		try {
			((RegistrationCI) this.offering).unregister(receptionPortURI);
		} catch (UnknownIdentifierException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RemoteException("Error: Connector unregister(receptionPortURI)", e);
		}
	}

	@Override
	public boolean channelAuthorised(String receptionPortURI, String channel) throws RemoteException {
		try {
			return ((RegistrationCI) this.offering).channelAuthorised(receptionPortURI, channel);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RemoteException("Error: Connector channelAuthorised", e);
		}
	}

	@Override
	public boolean channelExist(String channel) throws RemoteException {
		try {
			return ((RegistrationCI) this.offering).channelExist(channel);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RemoteException("Error: Connector channelExists(channel)", e);
		}
	}

	@Override
	public boolean subscribed(String receptionPortURI, String channel) throws RemoteException, UnknownChannelException {
		try {
			return ((RegistrationCI) this.offering).subscribed(receptionPortURI, channel);
		} catch (UnknownChannelException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RemoteException("Error: Connector subscribed(receptionPortURI, channel)", e);
		}
	}

	@Override
	public void subscribe(String receptionPortURI, String channel, MessageFilterI filter)
			throws RemoteException, UnknownChannelException {
		try {
			((RegistrationCI) this.offering).subscribe(receptionPortURI, channel, filter);
		} catch (UnknownChannelException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RemoteException("Error: Connector subscribe(receptionPortURI, channel, filter)", e);
		}
	}

	@Override
	public void unsubscribe(String receptionPortURI, String channel)
			throws RemoteException, UnknownChannelException, NotSubscribedChannelException {
		try {
			((RegistrationCI) this.offering).unsubscribe(receptionPortURI, channel);
		} catch (UnknownChannelException e) {
			throw e;
		} catch (NotSubscribedChannelException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RemoteException("Error: Connector unsubscribe(receptionPortURI, channel)", e);
		}
	}

	@Override
	public boolean modifyFilter(String receptionPortURI, String channel, MessageFilterI filter)
			throws RemoteException, UnknownChannelException, NotSubscribedChannelException {
		try {
			return ((RegistrationCI) this.offering).modifyFilter(receptionPortURI, channel, filter);
		} catch (UnknownChannelException e) {
			throw e;
		} catch (NotSubscribedChannelException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RemoteException("Error: Connector modifyFilter(receptionPortURI, channel, filter)", e);
		}
	}
}