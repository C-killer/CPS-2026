package fr.sorbonne_u.publication;

import java.time.Instant;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.messages.MessageFilter;
import fr.sorbonne_u.meteo.Position;
import fr.sorbonne_u.meteo.WindData;
import fr.sorbonne_u.publication.components.MeteoStation;
import fr.sorbonne_u.publication.components.Windmill;

/**
 * Test du protocole gossip en mode mono-JVM avec 2 courtiers interconnectes.
 *
 * <p>Scenario :</p>
 * <ul>
 *   <li>Broker-A et Broker-B sont connectes via gossip</li>
 *   <li>Un Windmill (subscriber) est enregistre sur Broker-B, abonne a channel0</li>
 *   <li>Une MeteoStation (publisher) est enregistree sur Broker-A, publie sur channel0</li>
 *   <li>Le message doit etre propage de Broker-A a Broker-B via gossip,
 *       puis delivre au Windmill sur Broker-B</li>
 * </ul>
 *
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class CVM_GossipTest extends AbstractCVM {

	public static final int NB_CHANNELS = 2;

	// Broker-A URIs
	public static final String BROKER_A_URI = "broker-A";
	public static final String BROKER_A_REG = "broker-A-registration";
	public static final String BROKER_A_PUB = "broker-A-publishing";
	public static final String BROKER_A_PRIV = "broker-A-privileged";
	public static final String BROKER_A_GOSSIP = "broker-A-gossip-receiver";

	// Broker-B URIs
	public static final String BROKER_B_URI = "broker-B";
	public static final String BROKER_B_REG = "broker-B-registration";
	public static final String BROKER_B_PUB = "broker-B-publishing";
	public static final String BROKER_B_PRIV = "broker-B-privileged";
	public static final String BROKER_B_GOSSIP = "broker-B-gossip-receiver";

	public CVM_GossipTest() throws Exception {
		super();
	}

	@Override
	public void deploy() throws Exception {

		// --- Broker-A (voisin : Broker-B) ---
		AbstractComponent.createComponent(
				Broker.class.getCanonicalName(),
				new Object[] {
						BROKER_A_URI,
						NB_CHANNELS,
						BROKER_A_REG,
						BROKER_A_PUB,
						BROKER_A_PRIV,
						BROKER_A_GOSSIP,
						new String[] { BROKER_B_GOSSIP }
				});

		// --- Broker-B (voisin : Broker-A) ---
		AbstractComponent.createComponent(
				Broker.class.getCanonicalName(),
				new Object[] {
						BROKER_B_URI,
						NB_CHANNELS,
						BROKER_B_REG,
						BROKER_B_PUB,
						BROKER_B_PRIV,
						BROKER_B_GOSSIP,
						new String[] { BROKER_A_GOSSIP }
				});

		// --- Windmill subscriber on Broker-B, channel0 ---
		AbstractComponent.createComponent(
				Windmill.class.getCanonicalName(),
				new Object[] {
						"windmill-on-B",
						BROKER_B_REG,          // enregistre aupres de Broker-B
						RegistrationClass.FREE,
						"channel0",
						new MessageFilter(null, null, null) // accept all
				});

		// --- MeteoStation publisher on Broker-A, channel0 ---
		WindData wd = new WindData(new Position(5, 5), 35.0, 20.0);
		Message windMsg = new Message(wd, Instant.now());
		windMsg.putProperty("type", "wind");
		windMsg.putProperty("speed", wd.force());
		windMsg.putProperty("zone", "north");

		AbstractComponent.createComponent(
				MeteoStation.class.getCanonicalName(),
				new Object[] {
						"station-on-A",
						BROKER_A_REG,          // enregistre aupres de Broker-A
						RegistrationClass.FREE,
						"channel0",
						windMsg
				});

		super.deploy();
	}

	public static void main(String[] args) {
		try {
			CVM_GossipTest cvm = new CVM_GossipTest();
			cvm.startStandardLifeCycle(8000L);
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
