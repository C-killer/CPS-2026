package fr.sorbonne_u.publication.gossip;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import fr.sorbonne_u.cps.gossip.interfaces.GossipMessageI;

/**
 * Implementation de {@link GossipMessageI} pour le protocole de bavardage
 * entre courtiers repartis.
 *
 * <p>Chaque message gossip contient :</p>
 * <ul>
 *   <li>un URI unique pour la deduplication</li>
 *   <li>un timestamp de creation</li>
 *   <li>l'URI du dernier emetteur (pour eviter le renvoi immediat)</li>
 *   <li>un type ({@link GossipPayloadType}) indiquant la nature de l'evenement</li>
 *   <li>une charge utile (payload) contenant les donnees specifiques</li>
 * </ul>
 *
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class GossipMessage implements GossipMessageI {

	private static final long serialVersionUID = 1L;

	private final String uri;
	private final Instant timestamp;
	private final String emitterURI;
	private final GossipPayloadType type;
	private final Map<String, Serializable> payload;

	/**
	 * Cree un nouveau message gossip.
	 *
	 * @param uri         URI unique du message
	 * @param emitterURI  URI du courtier emetteur
	 * @param type        type de l'evenement
	 * @param payload     donnees associees a l'evenement
	 */
	public GossipMessage(
			String uri,
			String emitterURI,
			GossipPayloadType type,
			Map<String, Serializable> payload) {
		this.uri = uri;
		this.timestamp = Instant.now();
		this.emitterURI = emitterURI;
		this.type = type;
		this.payload = payload != null ? payload : new HashMap<>();
	}

	/** Constructeur prive pour la copie superficielle. */
	private GossipMessage(
			String uri,
			Instant timestamp,
			String emitterURI,
			GossipPayloadType type,
			Map<String, Serializable> payload) {
		this.uri = uri;
		this.timestamp = timestamp;
		this.emitterURI = emitterURI;
		this.type = type;
		this.payload = payload;
	}

	@Override
	public String gossipMessageURI() {
		return uri;
	}

	@Override
	public Instant timestamp() {
		return timestamp;
	}

	@Override
	/*copyWithNewEmitterURI:
	When a broker receives a gossip message and prepares to forward it to a neighbor, 
	it should replace the emitterURI with its own URI. 
	This allows the neighbor, upon receiving the message, to identify the “previous sender,” 
	thereby preventing the message from being sent back to that broker (and avoiding a loop). */
	public GossipMessageI copyWithNewEmitterURI(String newGossipEmitterURI) {
		return new GossipMessage(
				this.uri,
				this.timestamp,
				newGossipEmitterURI,
				this.type,
				this.payload  // copie superficielle : meme reference
		);
	}

	public GossipPayloadType getType() {
		return type;
	}

	public String getEmitterURI() {
		return emitterURI;
	}

	public Serializable getPayloadValue(String key) {
		return payload.get(key);
	}

	@Override
	public String toString() {
		return "GossipMessage[uri=" + uri + ", type=" + type
				+ ", emitter=" + emitterURI + "]";
	}
}
