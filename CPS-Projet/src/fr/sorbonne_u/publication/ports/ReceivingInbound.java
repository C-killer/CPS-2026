package fr.sorbonne_u.publication.ports;

import java.rmi.RemoteException;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.publication.implementations.ReceivingImplI;

public class ReceivingInbound extends AbstractInboundPort implements ReceivingCI {

	private static final long serialVersionUID = 1L;

	public ReceivingInbound(ComponentI owner) throws Exception {
		super(ReceivingCI.class, owner);
		assert owner instanceof ReceivingImplI : "Owner must implement ReceivingImplI to use ReceivingInbound.";
	}

	public ReceivingInbound(String uri, ComponentI owner) throws Exception {
		super(uri, ReceivingCI.class, owner);
		assert owner instanceof ReceivingImplI : "Owner must implement ReceivingImplI to use ReceivingInbound.";
	}

	@Override
	public void receive(String channel, MessageI message) throws RemoteException {
		try {
			this.getOwner().handleRequest(o -> {
				((ReceivingImplI) o).receive(channel, message);
				return null;
			});
		} catch (Exception e) {
			throw new RemoteException("Error : Inbound receiving messages", e);
		}
	}

	@Override
	public void receive(String channel, MessageI[] messages) throws RemoteException {
		try {
			this.getOwner().handleRequest(o -> {
				((ReceivingImplI) o).receive(channel, messages);
				return null;
			});
		} catch (Exception e) {
			throw new RemoteException("Error : Inbound receiving messages", e);
		}
	}
}