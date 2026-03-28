package fr.sorbonne_u.publication.ports.inbound;

import java.rmi.RemoteException;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.publication.Client;
import fr.sorbonne_u.publication.implementations.ReceivingImplI;

/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class ReceivingInbound extends AbstractInboundPort implements ReceivingCI {

	private static final long serialVersionUID = 1L;

	public ReceivingInbound(ComponentI owner) throws Exception {
		super(ReceivingCI.class, owner);
		assert owner instanceof ReceivingImplI || owner instanceof Client
				: "Owner must implement ReceivingImplI or Client to use ReceivingInbound.";
	}

	public ReceivingInbound(String uri, ComponentI owner) throws Exception {
		super(uri, ReceivingCI.class, owner);
		assert owner instanceof ReceivingImplI || owner instanceof Client
				: "Owner must implement ReceivingImplI or Client to use ReceivingInbound.";
	}

	@Override
	public void receive(String channel, MessageI message) throws RemoteException {
		try {
			this.getOwner().runTask(o -> {
				try {
					if (o instanceof Client) {
						((Client) o).getSubscriptionPlugin().receive(channel, message);
					} else {
						((ReceivingImplI) o).receive(channel, message);
					}
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			});
		} catch (Exception e) {
			throw new RemoteException("Error scheduling async receive(message)", e);
		}
	}

	@Override
	public void receive(String channel, MessageI[] messages) throws RemoteException {
		try {
			this.getOwner().runTask(o -> {
				try {
					if (o instanceof Client) {
						((Client) o).getSubscriptionPlugin().receive(channel, messages);
					} else {
						((ReceivingImplI) o).receive(channel, messages);
					}
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			});
		} catch (Exception e) {
			throw new RemoteException("Error scheduling async receive(messages)", e);
		}
	}
}