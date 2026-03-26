package fr.sorbonne_u.publication.connectors;

import java.rmi.RemoteException;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.pubsub.exceptions.AlreadyRegisteredException;
import fr.sorbonne_u.cps.pubsub.exceptions.NotSubscribedChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownIdentifierException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.publication.implementations.RegistrationImplI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;

public class RegistrationConnector extends AbstractConnector implements RegistrationImplI {

	@Override // 检查client是否存在
	public boolean registered(String receptionPortURI) throws RemoteException {
		try {
			return ((RegistrationImplI) this.offering).registered(receptionPortURI);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RemoteException("Error: Connector registered(receptionPortURI)", e);
		}
	}

	@Override // 检查client是否以特定服务等级
	public boolean registered(String receptionPortURI, RegistrationClass rc) throws RemoteException {
		try {
			return ((RegistrationImplI) this.offering).registered(receptionPortURI, rc);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RemoteException("Error: Connector registered(receptionPortURI, rc)", e);
		}
	}

	@Override
	public String register(String receptionPortURI, RegistrationClass rc)
			throws RemoteException, AlreadyRegisteredException {
		try {
			return ((RegistrationImplI) this.offering).register(receptionPortURI, rc);
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
			return ((RegistrationImplI) this.offering).modifyServiceClass(receptionPortURI, rc);
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
			((RegistrationImplI) this.offering).unregister(receptionPortURI);
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
			return ((RegistrationImplI) this.offering).channelAuthorised(receptionPortURI, channel);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RemoteException("Error: Connector channelAuthorised", e);
		}
	}

	@Override
	public boolean channelExist(String channel) throws RemoteException {
		try {
			return ((RegistrationImplI) this.offering).channelExist(channel);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RemoteException("Error: Connector channelExists(channel)", e);
		}
	}

	@Override
	public boolean subscribed(String receptionPortURI, String channel) throws RemoteException, UnknownChannelException {
		try {
			return ((RegistrationImplI) this.offering).subscribed(receptionPortURI, channel);
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
			((RegistrationImplI) this.offering).subscribe(receptionPortURI, channel, filter);
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
			((RegistrationImplI) this.offering).unsubscribe(receptionPortURI, channel);
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
			return ((RegistrationImplI) this.offering).modifyFilter(receptionPortURI, channel, filter);
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