package fr.sorbonne_u.publication;

import java.time.Instant;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractDistributedCVM;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.messages.MessageFilter;
import fr.sorbonne_u.meteo.Position;
import fr.sorbonne_u.meteo.WindData;
import fr.sorbonne_u.publication.components.MeteoStation;
import fr.sorbonne_u.publication.components.Windmill;

/**
 * Deploiement reparti du systeme de publication/souscription sur 3 JVMs.
 *
 * <p>Topologie gossip (maillage complet) :</p>
 * <pre>
 *     JVM1 (Broker-1) &lt;----&gt; JVM2 (Broker-2)
 *           \                      /
 *            \                    /
 *             v                  v
 *              JVM3 (Broker-3)
 * </pre>
 *
 * <p>Chaque JVM contient un courtier local et des composants clients.
 * Les messages publies sur un courtier sont propages aux autres via gossip.</p>
 *
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class DistributedCVM extends AbstractDistributedCVM {

	// --- JVM URIs ---
	public static final String JVM1_URI = "jvm1";
	public static final String JVM2_URI = "jvm2";
	public static final String JVM3_URI = "jvm3";

	public static final int NB_CHANNELS = 2;

	// --- Broker-1 (JVM1) ---
	public static final String BROKER1_URI = "broker-1";
	public static final String BROKER1_REG = "broker1-registration";
	public static final String BROKER1_PUB = "broker1-publishing";
	public static final String BROKER1_PRIV = "broker1-privileged";
	public static final String BROKER1_GOSSIP = "broker1-gossip-receiver";

	// --- Broker-2 (JVM2) ---
	public static final String BROKER2_URI = "broker-2";
	public static final String BROKER2_REG = "broker2-registration";
	public static final String BROKER2_PUB = "broker2-publishing";
	public static final String BROKER2_PRIV = "broker2-privileged";
	public static final String BROKER2_GOSSIP = "broker2-gossip-receiver";

	// --- Broker-3 (JVM3) ---
	public static final String BROKER3_URI = "broker-3";
	public static final String BROKER3_REG = "broker3-registration";
	public static final String BROKER3_PUB = "broker3-publishing";
	public static final String BROKER3_PRIV = "broker3-privileged";
	public static final String BROKER3_GOSSIP = "broker3-gossip-receiver";

	/** Fichier de configuration par defaut. */
	public static final String DEFAULT_CONFIG = "config.xml";

	public DistributedCVM(String[] args) throws Exception {
		super(args);
	}

	/**
	 * Construit les arguments par defaut pour un JVM URI donne.
	 */
	private static String[] defaultArgs(String jvmURI) {
		return new String[] { jvmURI, DEFAULT_CONFIG };
	}

	@Override
	public void instantiateAndPublish() throws Exception {

		if (thisJVMURI.equals(JVM1_URI)) {
			// --- JVM1 : Broker-1 + publishers ---
			AbstractComponent.createComponent(
					Broker.class.getCanonicalName(),
					new Object[] {
							BROKER1_URI, NB_CHANNELS,
							BROKER1_REG, BROKER1_PUB, BROKER1_PRIV,
							BROKER1_GOSSIP,
							new String[] { BROKER2_GOSSIP, BROKER3_GOSSIP }
					});

			// Stations meteologiques (publishers FREE) sur Broker-1
			WindData wd1 = new WindData(new Position(1, 1), 40.0, 25.0);
			Message wind1 = new Message(wd1, Instant.now());
			wind1.putProperty("type", "wind");
			wind1.putProperty("speed", wd1.force());
			wind1.putProperty("zone", "north");

			AbstractComponent.createComponent(
					MeteoStation.class.getCanonicalName(),
					new Object[] {
							"station-jvm1",
							BROKER1_REG,
							RegistrationClass.FREE,
							"channel0",
							wind1
					});

		} else if (thisJVMURI.equals(JVM2_URI)) {
			// --- JVM2 : Broker-2 + subscribers ---
			AbstractComponent.createComponent(
					Broker.class.getCanonicalName(),
					new Object[] {
							BROKER2_URI, NB_CHANNELS,
							BROKER2_REG, BROKER2_PUB, BROKER2_PRIV,
							BROKER2_GOSSIP,
							new String[] { BROKER1_GOSSIP, BROKER3_GOSSIP }
					});

			// Windmill (subscriber FREE) sur Broker-2
			AbstractComponent.createComponent(
					Windmill.class.getCanonicalName(),
					new Object[] {
							"windmill-jvm2",
							BROKER2_REG,
							RegistrationClass.FREE,
							"channel0",
							new MessageFilter(null, null, null)
					});

		} else if (thisJVMURI.equals(JVM3_URI)) {
			// --- JVM3 : Broker-3 + mixed ---
			AbstractComponent.createComponent(
					Broker.class.getCanonicalName(),
					new Object[] {
							BROKER3_URI, NB_CHANNELS,
							BROKER3_REG, BROKER3_PUB, BROKER3_PRIV,
							BROKER3_GOSSIP,
							new String[] { BROKER1_GOSSIP, BROKER2_GOSSIP }
					});

			// Windmill (subscriber FREE) sur Broker-3
			AbstractComponent.createComponent(
					Windmill.class.getCanonicalName(),
					new Object[] {
							"windmill-jvm3",
							BROKER3_REG,
							RegistrationClass.FREE,
							"channel0",
							new MessageFilter(null, null, null)
					});

		} else {
			System.err.println("Unknown JVM URI: " + thisJVMURI);
		}

		super.instantiateAndPublish();
	}

	@Override
	public void interconnect() throws Exception {
		// Les connexions gossip sont faites dans Broker.execute()
		// via les neighborGossipInboundURIs
		super.interconnect();
	}

	@Override
	public void finalise() throws Exception {
		super.finalise();
	}

	@Override
	public void shutdown() throws Exception {
		super.shutdown();
	}

	/**
	 * Lancement : chaque JVM doit etre lancee separement.
	 *
	 * <p>Sans argument, lance les 3 JVMs par defaut (jvm1, jvm2, jvm3)
	 * avec config.xml. Sinon :</p>
	 * <pre>
	 *   java ... DistributedCVM jvm1 config.xml
	 * </pre>
	 * Il faut d'abord demarrer le GlobalRegistry et le CyclicBarrier BCM.
	 */
	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: DistributedCVM <jvmURI> [config.xml]");
			System.err.println("  jvmURI: jvm1 | jvm2 | jvm3");
			System.err.println("  config.xml defaults to: " + DEFAULT_CONFIG);
			System.err.println("  Lancer d'abord GlobalRegistry et CyclicBarrier.");
			System.exit(1);
		}
		// Si un seul argument (jvmURI), utiliser config.xml par defaut
		if (args.length == 1) {
			args = defaultArgs(args[0]);
		}
		try {
			DistributedCVM dcvm = new DistributedCVM(args);
			dcvm.startStandardLifeCycle(15000L);
			Thread.sleep(5000L);
			System.exit(0);
		} catch (Throwable e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
