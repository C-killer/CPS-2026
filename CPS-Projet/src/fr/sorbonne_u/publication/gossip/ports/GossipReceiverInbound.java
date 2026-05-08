package fr.sorbonne_u.publication.gossip.ports;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.gossip.interfaces.GossipImplementationI;
import fr.sorbonne_u.cps.gossip.interfaces.GossipMessageI;
import fr.sorbonne_u.cps.gossip.interfaces.GossipReceiverCI;

/**
 * Port entrant offrant l'interface {@link GossipReceiverCI} pour la
 * reception des messages gossip provenant des courtiers voisins.
 *
 * <p>La reception est executee de maniere asynchrone via {@code runTask}
 * pour ne pas bloquer l'appelant.</p>
 *
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class GossipReceiverInbound
		extends AbstractInboundPort
		implements GossipReceiverCI {

	private static final long serialVersionUID = 1L;

	public GossipReceiverInbound(ComponentI owner) throws Exception {
		super(GossipReceiverCI.class, owner);
		assert owner instanceof GossipImplementationI
				: "Owner must implement GossipImplementationI.";
	}

	public GossipReceiverInbound(String uri, ComponentI owner) throws Exception {
		super(uri, GossipReceiverCI.class, owner);
		assert owner instanceof GossipImplementationI
				: "Owner must implement GossipImplementationI.";
	}

	@Override
	public void receive(GossipMessageI[] gossipMessages) throws Exception {
		this.getOwner().runTask(o -> {
			try {
				((GossipImplementationI) o).receive(gossipMessages);
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
	}
}
