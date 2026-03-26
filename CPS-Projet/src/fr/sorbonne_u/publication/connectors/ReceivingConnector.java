package fr.sorbonne_u.publication.connectors;

import fr.sorbonne_u.components.connectors.AbstractConnector;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.publication.implementations.ReceivingImplI;

public class ReceivingConnector extends AbstractConnector implements ReceivingImplI {
	@Override
	public void receive(String channel, MessageI message) throws Exception {
		((ReceivingImplI) this.offering).receive(channel, message);
	}

	@Override
	public void receive(String channel, MessageI[] messages) throws Exception {
		((ReceivingImplI) this.offering).receive(channel, messages);
	}
}
