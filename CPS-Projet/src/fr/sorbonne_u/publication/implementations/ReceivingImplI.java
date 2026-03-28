package fr.sorbonne_u.publication.implementations;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;

/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public interface ReceivingImplI {
	void receive(String channel, MessageI message) throws Exception;

	void receive(String channel, MessageI[] messages) throws Exception;
}
