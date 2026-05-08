package fr.sorbonne_u.publication.gossip.ports;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.gossip.interfaces.GossipMessageI;
import fr.sorbonne_u.cps.gossip.interfaces.GossipSenderCI;

/**
 * Port sortant requerant l'interface {@link GossipSenderCI} pour
 * l'envoi de messages gossip vers un courtier voisin.
 *
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class GossipSenderOutbound
		extends AbstractOutboundPort
		implements GossipSenderCI {

	private static final long serialVersionUID = 1L;

	public GossipSenderOutbound(ComponentI owner) throws Exception {
		super(GossipSenderCI.class, owner);
	}

	public GossipSenderOutbound(String uri, ComponentI owner) throws Exception {
		super(uri, GossipSenderCI.class, owner);
	}

	@Override
	public void send(GossipMessageI[] gossipMessages) throws Exception {
		((GossipSenderCI) this.getConnector()).send(gossipMessages);
	}
}
