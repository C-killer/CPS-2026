package fr.sorbonne_u.publication.connectors;

import fr.sorbonne_u.components.connectors.AbstractConnector;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;

/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class ReceivingConnector extends AbstractConnector implements ReceivingCI {
	@Override
	public void receive(String channel, MessageI message) throws Exception {
		((ReceivingCI) this.offering).receive(channel, message);
	}

	@Override
	public void receive(String channel, MessageI[] messages) throws Exception {
		((ReceivingCI) this.offering).receive(channel, messages);
	}
}
