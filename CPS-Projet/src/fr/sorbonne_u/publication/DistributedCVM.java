package fr.sorbonne_u.publication;

import java.time.Duration;
import java.time.Instant;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractDistributedCVM;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.messages.MessageFilter;
import fr.sorbonne_u.meteo.MeteoAlert;
import fr.sorbonne_u.meteo.Position;
import fr.sorbonne_u.meteo.RectangularRegion;
import fr.sorbonne_u.meteo.WindData;
import fr.sorbonne_u.publication.components.MeteoOffice;
import fr.sorbonne_u.publication.components.MeteoStation;
import fr.sorbonne_u.publication.components.Windmill;

/**
 * Deploiement reparti sur 5 JVMs avec topologie gossip en anneau + raccourci.
 *
 * <p>Topologie gossip (pas de maillage complet, multi-hop) :</p>
 * <pre>
 *     Broker-A ---- Broker-B
 *        |             |
 *     Broker-E ---- Broker-C
 *          \        /
 *          Broker-D
 * </pre>
 *
 * <p>Repartition :</p>
 * <ul>
 *   <li>JVM1 (Broker-A) : 3 MeteoStation publishers (FREE, channel0)</li>
 *   <li>JVM2 (Broker-B) : 4 Windmill subscribers (FREE, channel0, strongWindFilter)</li>
 *   <li>JVM3 (Broker-C) : 1 MeteoOffice alert publisher (PREMIUM, channel1)
 *                        + 1 MeteoStation publisher (FREE, channel0)</li>
 *   <li>JVM4 (Broker-D) : 2 Windmill subscribers (STANDARD, channel0)
 *                        + 1 desk subscriber (PREMIUM, channel1)</li>
 *   <li>JVM5 (Broker-E) : 3 Windmill subscribers (FREE, channel0, strongWindFilter)
 *                        + 2 desk subscribers (PREMIUM, channel1, alertFilter)
 *                        + 1 late publisher (FREE, channel0, weak wind)</li>
 * </ul>
 *
 * <p>Fonctionnalites demontrees :</p>
 * <ol>
 *   <li>Propagation multi-hop (A→B→C→D, A→E→D) avec deduplication sur D</li>
 *   <li>3 classes de service (FREE, STANDARD, PREMIUM) avec pools differencies</li>
 *   <li>Filtrage des messages (strongWindFilter : vent &gt;30 accepte, &lt;=30 rejete)</li>
 *   <li>Alertes cross-JVM multi-hop (channel1 : C→B→A, C→E, C→D)</li>
 *   <li>Weak wind filtre (station-E1 speed=10 ne passe pas strongWindFilter)</li>
 * </ol>
 * <p>Note : la creation dynamique de canaux est demontree dans
 * {@link CVM_ScenarioTest} avec TestScenario (coordination temporelle).</p>
 *
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class DistributedCVM extends AbstractDistributedCVM {

	// --- JVM URIs ---
	public static final String JVM1_URI = "jvm1";
	public static final String JVM2_URI = "jvm2";
	public static final String JVM3_URI = "jvm3";
	public static final String JVM4_URI = "jvm4";
	public static final String JVM5_URI = "jvm5";

	public static final int NB_CHANNELS = 2; // channel0, channel1

	// --- Broker-A (JVM1) ---
	public static final String BA_URI = "broker-A";
	public static final String BA_REG = "brokerA-registration";
	public static final String BA_PUB = "brokerA-publishing";
	public static final String BA_PRIV = "brokerA-privileged";
	public static final String BA_GOSSIP = "brokerA-gossip-receiver";

	// --- Broker-B (JVM2) ---
	public static final String BB_URI = "broker-B";
	public static final String BB_REG = "brokerB-registration";
	public static final String BB_PUB = "brokerB-publishing";
	public static final String BB_PRIV = "brokerB-privileged";
	public static final String BB_GOSSIP = "brokerB-gossip-receiver";

	// --- Broker-C (JVM3) ---
	public static final String BC_URI = "broker-C";
	public static final String BC_REG = "brokerC-registration";
	public static final String BC_PUB = "brokerC-publishing";
	public static final String BC_PRIV = "brokerC-privileged";
	public static final String BC_GOSSIP = "brokerC-gossip-receiver";

	// --- Broker-D (JVM4) ---
	public static final String BD_URI = "broker-D";
	public static final String BD_REG = "brokerD-registration";
	public static final String BD_PUB = "brokerD-publishing";
	public static final String BD_PRIV = "brokerD-privileged";
	public static final String BD_GOSSIP = "brokerD-gossip-receiver";

	// --- Broker-E (JVM5) ---
	public static final String BE_URI = "broker-E";
	public static final String BE_REG = "brokerE-registration";
	public static final String BE_PUB = "brokerE-publishing";
	public static final String BE_PRIV = "brokerE-privileged";
	public static final String BE_GOSSIP = "brokerE-gossip-receiver";

	/*
	 * Topologie gossip (anneau + raccourci) :
	 *   A -- B
	 *   |    |
	 *   E -- C
	 *    \  /
	 *     D
	 *
	 * A: voisins B, E
	 * B: voisins A, C
	 * C: voisins B, E, D
	 * D: voisins C, E
	 * E: voisins A, C, D
	 */

	public static final String DEFAULT_CONFIG = "config.xml";

	public DistributedCVM(String[] args) throws Exception {
		super(args);
	}

	private static String[] defaultArgs(String jvmURI) {
		return new String[] { jvmURI, DEFAULT_CONFIG };
	}

	// =====================================================================
	// Message / filter factories
	// =====================================================================

	static Message windMessage(double x, double y, int speed, String zone) throws Exception {
		double vx = "east".equals(zone) ? speed : "west".equals(zone) ? -speed : 0;
		double vy = "north".equals(zone) ? speed : "south".equals(zone) ? -speed : 0;
		WindData wd = new WindData(new Position(x, y), vx, vy);
		Message m = new Message(wd, Instant.now());
		m.putProperty("type", "wind");
		m.putProperty("speed", wd.force());
		m.putProperty("zone", zone);
		return m;
	}

	static Message alertMessage(String level) throws Exception {
		MeteoAlertI.Level lv;
		switch (level) {
			case "red": lv = MeteoAlertI.Level.RED; break;
			case "yellow": lv = MeteoAlertI.Level.YELLOW; break;
			case "green": lv = MeteoAlertI.Level.GREEN; break;
			default: lv = MeteoAlertI.Level.ORANGE;
		}
		MeteoAlert alert = new MeteoAlert(
				MeteoAlertI.AlertType.STORM, lv,
				new RectangularRegion[] {
						new RectangularRegion(new Position(-100, -100), new Position(100, 100))
				},
				Instant.now(), Duration.ofHours(6));
		Message m = new Message(alert, Instant.now());
		m.putProperty("type", "alert");
		m.putProperty("level", level);
		return m;
	}

	static Message dataMessage(String desc) throws Exception {
		Message m = new Message(desc, Instant.now());
		m.putProperty("type", "data");
		m.putProperty("source", desc);
		return m;
	}

	static MessageFilterI strongWindFilter() {
		return new MessageFilter(
				new MessageFilter.PropertyFilterI[] {
						new MessageFilter.PropertyFilter("speed",
								new MessageFilter.ValueFilter.GreaterThanFilter(30.0))
				}, null, null);
	}

	static MessageFilterI alertFilter() {
		return new MessageFilter(
				new MessageFilter.PropertyFilterI[] {
						new MessageFilter.PropertyFilter("type",
								new MessageFilter.ValueFilter("alert"))
				}, null, null);
	}

	// =====================================================================
	// Deploy
	// =====================================================================

	@Override
	public void instantiateAndPublish() throws Exception {

		if (thisJVMURI.equals(JVM1_URI)) {
			// =============================================================
			// JVM1 : Broker-A + 3 MeteoStation publishers (FREE, channel0)
			// Voisins gossip : B, E
			// =============================================================
			AbstractComponent.createComponent(
					Broker.class.getCanonicalName(),
					new Object[] {
							BA_URI, NB_CHANNELS,
							BA_REG, BA_PUB, BA_PRIV,
							BA_GOSSIP,
							new String[] { BB_GOSSIP, BE_GOSSIP }
					});

			// 3 stations meteo : strong wind (>30)
			AbstractComponent.createComponent(
					MeteoStation.class.getCanonicalName(),
					new Object[] { "station-A1", BA_REG, RegistrationClass.FREE,
							"channel0", windMessage(2, 3, 45, "north") });
			AbstractComponent.createComponent(
					MeteoStation.class.getCanonicalName(),
					new Object[] { "station-A2", BA_REG, RegistrationClass.FREE,
							"channel0", windMessage(5, 8, 38, "east") });
			AbstractComponent.createComponent(
					MeteoStation.class.getCanonicalName(),
					new Object[] { "station-A3", BA_REG, RegistrationClass.FREE,
							"channel0", windMessage(-3, 1, 52, "west") });

		} else if (thisJVMURI.equals(JVM2_URI)) {
			// =============================================================
			// JVM2 : Broker-B + 4 Windmill subscribers (FREE, channel0)
			// Voisins gossip : A, C
			// =============================================================
			AbstractComponent.createComponent(
					Broker.class.getCanonicalName(),
					new Object[] {
							BB_URI, NB_CHANNELS,
							BB_REG, BB_PUB, BB_PRIV,
							BB_GOSSIP,
							new String[] { BA_GOSSIP, BC_GOSSIP }
					});

			for (int i = 1; i <= 4; i++) {
				AbstractComponent.createComponent(
						Windmill.class.getCanonicalName(),
						new Object[] { "windmill-B" + i, BB_REG, RegistrationClass.FREE,
								"channel0", strongWindFilter() });
			}

		} else if (thisJVMURI.equals(JVM3_URI)) {
			// =============================================================
			// JVM3 : Broker-C + 1 MeteoOffice (PREMIUM pub ch1)
			//                  + 1 MeteoStation (FREE pub ch0)
			// Voisins gossip : B, E, D
			// C est un hub : 3 voisins, teste le fan-out gossip
			// =============================================================
			AbstractComponent.createComponent(
					Broker.class.getCanonicalName(),
					new Object[] {
							BC_URI, NB_CHANNELS,
							BC_REG, BC_PUB, BC_PRIV,
							BC_GOSSIP,
							new String[] { BB_GOSSIP, BE_GOSSIP, BD_GOSSIP }
					});

			// Alert publisher → channel1 (propage C→B→A, C→E, C→D)
			AbstractComponent.createComponent(
					MeteoOffice.class.getCanonicalName(),
					new Object[] { "office-C1", BC_REG, RegistrationClass.PREMIUM,
							"channel1", alertMessage("orange") });

			// Station publisher → channel0 (teste publication depuis un noeud central)
			AbstractComponent.createComponent(
					MeteoStation.class.getCanonicalName(),
					new Object[] { "station-C1", BC_REG, RegistrationClass.FREE,
							"channel0", windMessage(20, 20, 40, "north") });

		} else if (thisJVMURI.equals(JVM4_URI)) {
			// =============================================================
			// JVM4 : Broker-D + 2 Windmill subscribers (STANDARD, ch0)
			//                  + 1 desk subscriber (PREMIUM, ch1)
			// Voisins gossip : C, E
			// D est le noeud le plus eloigne de A (multi-hop: A→B→C→D ou A→E→D)
			// =============================================================
			AbstractComponent.createComponent(
					Broker.class.getCanonicalName(),
					new Object[] {
							BD_URI, NB_CHANNELS,
							BD_REG, BD_PUB, BD_PRIV,
							BD_GOSSIP,
							new String[] { BC_GOSSIP, BE_GOSSIP }
					});

			// 2 STANDARD subscribers channel0 (teste livraison multi-hop + pool STANDARD)
			for (int i = 1; i <= 2; i++) {
				AbstractComponent.createComponent(
						Windmill.class.getCanonicalName(),
						new Object[] { "monitor-D" + i, BD_REG, RegistrationClass.STANDARD,
								"channel0", strongWindFilter() });
			}

			// 1 PREMIUM desk channel1 (alert arrive par multi-hop C→D)
			AbstractComponent.createComponent(
					Windmill.class.getCanonicalName(),
					new Object[] { "desk-D1", BD_REG, RegistrationClass.PREMIUM,
							"channel1", alertFilter() });

		} else if (thisJVMURI.equals(JVM5_URI)) {
			// =============================================================
			// JVM5 : Broker-E + 3 windmills (FREE sub ch0)
			//                  + 2 desks (PREMIUM sub ch1)
			//                  + 1 late publisher (FREE, ch0, weak wind)
			// Voisins gossip : A, C, D
			// =============================================================
			AbstractComponent.createComponent(
					Broker.class.getCanonicalName(),
					new Object[] {
							BE_URI, NB_CHANNELS,
							BE_REG, BE_PUB, BE_PRIV,
							BE_GOSSIP,
							new String[] { BA_GOSSIP, BC_GOSSIP, BD_GOSSIP }
					});

			// 3 windmills → channel0 (strongWindFilter)
			for (int i = 1; i <= 3; i++) {
				AbstractComponent.createComponent(
						Windmill.class.getCanonicalName(),
						new Object[] { "windmill-E" + i, BE_REG, RegistrationClass.FREE,
								"channel0", strongWindFilter() });
			}

			// 2 desks → channel1 (alertFilter)
			for (int i = 1; i <= 2; i++) {
				AbstractComponent.createComponent(
						Windmill.class.getCanonicalName(),
						new Object[] { "desk-E" + i, BE_REG, RegistrationClass.PREMIUM,
								"channel1", alertFilter() });
			}

			// 1 late publisher → channel0 (weak wind, speed=10, sera filtre)
			AbstractComponent.createComponent(
					MeteoStation.class.getCanonicalName(),
					new Object[] { "station-E1", BE_REG, RegistrationClass.FREE,
							"channel0", windMessage(50, 50, 10, "south") });

		} else {
			System.err.println("Unknown JVM URI: " + thisJVMURI);
		}

		super.instantiateAndPublish();
	}

	@Override
	public void interconnect() throws Exception {
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
	 * Lancement :
	 * <pre>
	 *   java ... DistributedCVM jvm1 [config.xml]
	 *   ...
	 *   java ... DistributedCVM jvm5 [config.xml]
	 * </pre>
	 */
	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: DistributedCVM <jvmURI> [config.xml]");
			System.err.println("  jvmURI: jvm1 | jvm2 | jvm3 | jvm4 | jvm5");
			System.exit(1);
		}
		if (args.length == 1) {
			args = defaultArgs(args[0]);
		}
		try {
			DistributedCVM dcvm = new DistributedCVM(args);
			dcvm.startStandardLifeCycle(20000L);
			Thread.sleep(5000L);
			System.exit(0);
		} catch (Throwable e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
