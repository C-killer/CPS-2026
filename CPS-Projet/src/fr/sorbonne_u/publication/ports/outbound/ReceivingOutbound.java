package fr.sorbonne_u.publication.ports.outbound;

import java.rmi.RemoteException;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;

/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class ReceivingOutbound extends AbstractOutboundPort implements ReceivingCI {

	private static final long serialVersionUID = 1L;

	public ReceivingOutbound(ComponentI owner) throws Exception {
		super(ReceivingCI.class, owner);
	}

	public ReceivingOutbound(String uri, ComponentI owner) throws Exception {
		super(uri, ReceivingCI.class, owner);
	}

	@Override
	public void receive(String channel, MessageI message) throws RemoteException {
		try {
			((ReceivingCI) this.getConnector()).receive(channel, message);
		} catch (RemoteException e) {
			throw e;
		} catch (Exception e) {
			throw new RemoteException("Error : Outbound receiving message", e);
		}

	}

	@Override
	public void receive(String channel, MessageI[] messages) throws RemoteException {
		try {
			((ReceivingCI) this.getConnector()).receive(channel, messages);
		} catch (RemoteException e) {
			throw e;
		} catch (Exception e) {
			throw new RemoteException("Error : Outbound receiving messages", e);
		}
	}
}