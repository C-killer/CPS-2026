package fr.sorbonne_u.publication.gossip.connectors;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.gossip.interfaces.GossipMessageI;
import fr.sorbonne_u.cps.gossip.interfaces.GossipReceiverCI;
import fr.sorbonne_u.cps.gossip.interfaces.GossipSenderCI;

/**
 * Connecteur reliant un port sortant {@link GossipSenderCI} a un port
 * entrant {@link GossipReceiverCI} pour le protocole de bavardage.
 *
 * <p>L'appel a {@code send()} cote emetteur est traduit en appel a
 * {@code receive()} cote recepteur.</p>
 *
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class GossipConnector
		extends AbstractConnector
		implements GossipSenderCI {

	@Override
	public void send(GossipMessageI[] gossipMessages) throws Exception {
		((GossipReceiverCI) this.offering).receive(gossipMessages);
	}
}
