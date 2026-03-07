package fr.sorbonne_u.publication.ports.inbound;

import java.rmi.RemoteException;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.publication.Client;
import fr.sorbonne_u.publication.implementations.ReceivingImplI;

public class ReceivingInbound extends AbstractInboundPort implements ReceivingCI {

	private static final long serialVersionUID = 1L;

	public ReceivingInbound(ComponentI owner) throws Exception {
		super(ReceivingCI.class, owner);
		assert owner instanceof Client : "Owner must implement Client to use ReceivingInbound.";
	}

	public ReceivingInbound(String uri, ComponentI owner) throws Exception {
		super(uri, ReceivingCI.class, owner);
		assert owner instanceof Client : "Owner must implement Client to use ReceivingInbound.";
	}

	// compatibility layer:
	// supports both legacy components (Audit1) and plugin-based clients
	@Override
	public void receive(String channel, MessageI message) throws RemoteException {
		try {
			this.getOwner().handleRequest(o -> {

				if (o instanceof Client) {
					// new plugin client
					Client c = (Client) o;
					c.getSubscriptionPlugin().receive(channel, message);

				} else {
					// old audit1 client
					((ReceivingImplI) o).receive(channel, message);
				}

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

				if (o instanceof Client) {
					Client c = (Client) o;
					c.getSubscriptionPlugin().receive(channel, messages);

				} else {
					((ReceivingImplI) o).receive(channel, messages);
				}

				return null;
			});
		} catch (Exception e) {
			throw new RemoteException("Error : Inbound receiving messages", e);
		}
	}
}