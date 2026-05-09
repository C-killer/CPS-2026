package fr.sorbonne_u.publication;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.cps.gossip.interfaces.GossipImplementationI;
import fr.sorbonne_u.cps.gossip.interfaces.GossipMessageI;
import fr.sorbonne_u.cps.gossip.interfaces.GossipReceiverCI;
import fr.sorbonne_u.cps.gossip.interfaces.GossipSenderCI;
import fr.sorbonne_u.cps.pubsub.exceptions.AlreadyExistingChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.AlreadyRegisteredException;
import fr.sorbonne_u.cps.pubsub.exceptions.NotSubscribedChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnauthorisedClientException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownClientException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownIdentifierException;
import fr.sorbonne_u.cps.pubsub.interfaces.ChannelQuotaExceededException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.publication.connectors.AbnormalTerminationNotificationConnector;
import fr.sorbonne_u.publication.connectors.ReceivingConnector;
import fr.sorbonne_u.publication.gossip.GossipMessage;
import fr.sorbonne_u.publication.gossip.GossipPayloadType;
import fr.sorbonne_u.publication.gossip.connectors.GossipConnector;
import fr.sorbonne_u.publication.gossip.ports.GossipReceiverInbound;
import fr.sorbonne_u.publication.gossip.ports.GossipSenderOutbound;
import fr.sorbonne_u.publication.implementations.PrivilegedClientImpli;
import fr.sorbonne_u.publication.implementations.RegistrationImplI;
import fr.sorbonne_u.publication.interfaces.AbnormalTerminationNotificationCI;
import fr.sorbonne_u.publication.ports.inbound.PrivilegedClientInbound;
import fr.sorbonne_u.publication.ports.inbound.PublishingInbound;
import fr.sorbonne_u.publication.ports.inbound.RegistrationInbound;
import fr.sorbonne_u.publication.ports.outbound.AbnormalTerminationNotificationOutbound;
import fr.sorbonne_u.publication.ports.outbound.ReceivingOutbound;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;

/**
 * Central broker component managing registration, subscriptions, channels
 * and message delivery.
 *
 * <p>
 * Thread-safety: all mutable shared state uses {@link ConcurrentHashMap}.
 * Compound check-then-act operations use fine-grained locks:
 * {@code registrationLock} for register/unregister,
 * {@code channelLock} for create/destroy channel.
 * Outbound port caches use optimistic putIfAbsent.
 * </p>
 *
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class Broker extends AbstractComponent
		implements RegistrationImplI, PrivilegedClientImpli, GossipImplementationI {

	protected final RegistrationInbound registrationInboundPort;
	protected final PublishingInbound publishingInboundPort;
	protected final PrivilegedClientInbound privilegedClientInboundPort;

	// --- Port URIs (instance-specific for multi-broker support) ---
	protected final String registrationInboundURI;
	protected final String publishingInboundURI;
	protected final String privilegedInboundURI;

	// Default URIs for backward compatibility (single-broker)
	protected static final String REGISTRATION_INBOUND_URI = "broker-registration";
	protected static final String PUBLISHING_INBOUND_URI = "broker-publishing";
	protected static final String PRIVILEGED_INBOUND_URI = "broker-privileged";

	// --- Fine-grained locks (replace global synchronized(this)) ---
	private final Object registrationLock = new Object();
	private final Object channelLock = new Object();

	// --- Shared mutable state (all ConcurrentHashMap for safe concurrent reads)
	// ---
	protected final Set<String> channels = ConcurrentHashMap.newKeySet();
	protected final ConcurrentMap<String, String> channelCreators = new ConcurrentHashMap<>();
	protected final ConcurrentMap<String, String> channelAuthorisations = new ConcurrentHashMap<>();
	protected final ConcurrentMap<String, Set<String>> createdChannelsByClient = new ConcurrentHashMap<>();
	protected final ConcurrentMap<String, RegistrationClass> registered = new ConcurrentHashMap<>();
	protected final ConcurrentMap<String, Map<String, MessageFilterI>> subscriptions = new ConcurrentHashMap<>();
	protected final ConcurrentMap<String, ReceivingOutbound> receivingOutboundPorts = new ConcurrentHashMap<>();

	/** Cached outbound ports for abnormal-termination notifications. */
	protected final ConcurrentMap<String, AbnormalTerminationNotificationOutbound> notificationOutboundPorts = new ConcurrentHashMap<>();

	protected static final int STANDARD_QUOTA = 3;
	protected static final int PREMIUM_QUOTA = 10;

	// --- BCM executor service URIs (§3.6.3) ---
	public static final String PUBLICATION_HANDLER_URI = "publication-pool";
	public static final String PROPAGATION_HANDLER_URI = "propagation-pool";
	public static final String DELIVERY_PREMIUM_HANDLER_URI = "delivery-premium-pool";
	public static final String DELIVERY_STANDARD_HANDLER_URI = "delivery-standard-pool";
	public static final String DELIVERY_FREE_HANDLER_URI = "delivery-free-pool";
	public static final String GOSSIP_HANDLER_URI = "gossip-pool";

	// --- Gossip protocol fields (§3.7) ---
	protected final String brokerURI;
	/**
	 * URI du port entrant gossip de CE courtier, utilise comme emitterURI dans les
	 * messages gossip.
	 */
	protected String gossipReceiverInboundURI;
	protected GossipReceiverInbound gossipReceiverInbound;
	protected final ConcurrentMap<String, GossipSenderOutbound> gossipSenderOutbounds = new ConcurrentHashMap<>();

	/*
	 * processedGossipURIs:
	 * Processed gossip message URI → timestamp,
	 * used for deduplication
	 */
	protected final ConcurrentMap<String, Instant> processedGossipURIs = new ConcurrentHashMap<>();

	/** URIs des ports entrants gossip des voisins, pour connexion dans start(). */
	protected final String[] neighborGossipInboundURIs;

	/*
	 * Interval for clearing expired URIs and maximum
	 * retention period
	 */
	private static final long GOSSIP_CLEANUP_INTERVAL_MS = 30_000L;
	private static final long GOSSIP_URI_MAX_AGE_MS = 60_000L;

	// =========================================================================
	// Construction / lifecycle
	// =========================================================================

	/**
	 * Constructeur pour le mode mono-courtier (backward compatible).
	 */
	protected Broker(int nbChannels) throws Exception {
		this("broker-local", nbChannels, REGISTRATION_INBOUND_URI,
				PUBLISHING_INBOUND_URI, PRIVILEGED_INBOUND_URI,
				null, new String[0]);
	}

	/**
	 * Constructeur complet pour le mode multi-courtier reparti.
	 *
	 * @param brokerURI                 URI unique de ce courtier
	 * @param nbChannels                nombre de canaux pre-crees (FREE)
	 * @param registrationInboundURI    URI du port entrant Registration
	 * @param publishingInboundURI      URI du port entrant Publishing
	 * @param privilegedInboundURI      URI du port entrant PrivilegedClient
	 * @param gossipReceiverInboundURI  URI du port entrant Gossip (null si pas de
	 *                                  gossip)
	 * @param neighborGossipInboundURIs URIs des ports gossip des voisins
	 */
	protected Broker(
			String brokerURI,
			int nbChannels,
			String registrationInboundURI,
			String publishingInboundURI,
			String privilegedInboundURI,
			String gossipReceiverInboundURI,
			String[] neighborGossipInboundURIs) throws Exception {
		super(5, 1);

		this.brokerURI = brokerURI;
		this.registrationInboundURI = registrationInboundURI;
		this.publishingInboundURI = publishingInboundURI;
		this.privilegedInboundURI = privilegedInboundURI;
		this.gossipReceiverInboundURI = gossipReceiverInboundURI;
		this.neighborGossipInboundURIs = neighborGossipInboundURIs != null
				? neighborGossipInboundURIs
				: new String[0];

		this.addOfferedInterface(RegistrationCI.class);
		this.addOfferedInterface(PublishingCI.class);
		this.addOfferedInterface(PrivilegedClientCI.class);
		this.addRequiredInterface(ReceivingCI.class);
		this.addRequiredInterface(AbnormalTerminationNotificationCI.class);

		// BCM managed executor services
		this.createNewExecutorService(PUBLICATION_HANDLER_URI, 2, false);
		this.createNewExecutorService(PROPAGATION_HANDLER_URI, 4, false);
		this.createNewExecutorService(DELIVERY_PREMIUM_HANDLER_URI, 4, false);
		this.createNewExecutorService(DELIVERY_STANDARD_HANDLER_URI, 2, false);
		this.createNewExecutorService(DELIVERY_FREE_HANDLER_URI, 1, false);
		this.createNewExecutorService(GOSSIP_HANDLER_URI, 2, true);

		for (int i = 0; i < nbChannels; i++) {
			String c = "channel" + i;
			channels.add(c);
			channelCreators.put(c, "SYSTEM");
			channelAuthorisations.put(c, ".*");
		}

		this.registrationInboundPort = new RegistrationInbound(registrationInboundURI, this);
		this.registrationInboundPort.publishPort();

		this.publishingInboundPort = new PublishingInbound(publishingInboundURI, this);
		this.publishingInboundPort.publishPort();

		this.privilegedClientInboundPort = new PrivilegedClientInbound(privilegedInboundURI, this);
		this.privilegedClientInboundPort.publishPort();

		// Gossip protocol: port entrant recepteur
		if (gossipReceiverInboundURI != null) {
			this.addOfferedInterface(GossipReceiverCI.class);
			this.addRequiredInterface(GossipSenderCI.class);
			this.gossipReceiverInbound = new GossipReceiverInbound(gossipReceiverInboundURI, this);
			this.gossipReceiverInbound.publishPort();
		}
	}

	@Override
	public void execute() throws Exception {
		// Connexion gossip AVANT super.execute() pour garantir que les
		// voisins sont connectes avant toute activite client
		if (neighborGossipInboundURIs.length > 0) {
			for (String neighborURI : neighborGossipInboundURIs) {
				GossipSenderOutbound gso = new GossipSenderOutbound(this);
				gso.publishPort();
				gso.doConnection(neighborURI, GossipConnector.class.getCanonicalName());
				gossipSenderOutbounds.put(neighborURI, gso);
			}
			System.out.println("[Broker " + brokerURI + "] connected to "
					+ gossipSenderOutbounds.size() + " gossip neighbors.");

			// Nettoyage periodique des URIs gossip deja traites
			this.scheduleTaskAtFixedRate(
					GOSSIP_HANDLER_URI,
					c -> {
						Instant cutoff = Instant.now().minusMillis(GOSSIP_URI_MAX_AGE_MS);
						processedGossipURIs.entrySet()
								.removeIf(e -> e.getValue().isBefore(cutoff));
					},
					GOSSIP_CLEANUP_INTERVAL_MS,
					GOSSIP_CLEANUP_INTERVAL_MS,
					TimeUnit.MILLISECONDS);
		}

		super.execute();
	}

	@Override
	public void finalise() throws Exception {
		for (ReceivingOutbound rop : receivingOutboundPorts.values()) {
			try {
				rop.doDisconnection();
			} catch (Throwable ignored) {
			}
			try {
				rop.unpublishPort();
			} catch (Throwable ignored) {
			}
		}
		receivingOutboundPorts.clear();

		for (AbnormalTerminationNotificationOutbound np : notificationOutboundPorts.values()) {
			try {
				np.doDisconnection();
			} catch (Throwable ignored) {
			}
			try {
				np.unpublishPort();
			} catch (Throwable ignored) {
			}
		}
		notificationOutboundPorts.clear();

		// Gossip outbound ports
		for (GossipSenderOutbound gso : gossipSenderOutbounds.values()) {
			try {
				gso.doDisconnection();
			} catch (Throwable ignored) {
			}
			try {
				gso.unpublishPort();
			} catch (Throwable ignored) {
			}
		}
		gossipSenderOutbounds.clear();

		super.finalise();
	}

	@Override
	public void shutdown() throws ComponentShutdownException {
		try {
			this.registrationInboundPort.unpublishPort();
		} catch (Throwable ignored) {
		}
		try {
			this.publishingInboundPort.unpublishPort();
		} catch (Throwable ignored) {
		}
		try {
			this.privilegedClientInboundPort.unpublishPort();
		} catch (Throwable ignored) {
		}
		if (this.gossipReceiverInbound != null) {
			try {
				this.gossipReceiverInbound.unpublishPort();
			} catch (Throwable ignored) {
			}
		}
		for (ReceivingOutbound rop : receivingOutboundPorts.values()) {
			try {
				rop.unpublishPort();
			} catch (Throwable ignored) {
			}
		}
		for (AbnormalTerminationNotificationOutbound np : notificationOutboundPorts.values()) {
			try {
				np.unpublishPort();
			} catch (Throwable ignored) {
			}
		}
		for (GossipSenderOutbound gso : gossipSenderOutbounds.values()) {
			try {
				gso.unpublishPort();
			} catch (Throwable ignored) {
			}
		}
		super.shutdown();
	}

	/** Retourne l'URI du port Registration (backward compatible). */
	public static String registrationPortURI() {
		return REGISTRATION_INBOUND_URI;
	}

	/** Retourne l'URI du port Registration de cette instance. */
	public String getRegistrationInboundURI() {
		return registrationInboundURI;
	}

	/** Retourne l'URI du port Publishing de cette instance. */
	public String getPublishingInboundURI() {
		return publishingInboundURI;
	}

	/** Retourne l'URI du port PrivilegedClient de cette instance. */
	public String getPrivilegedInboundURI() {
		return privilegedInboundURI;
	}

	// =========================================================================
	// Registration (synchronized for compound check-then-act)
	// =========================================================================

	@Override
	public boolean registered(String receptionPortURI) throws Exception {
		return registered.containsKey(receptionPortURI);
	}

	@Override
	public boolean registered(String receptionPortURI, RegistrationClass rc) throws Exception {
		return rc != null && rc.equals(registered.get(receptionPortURI));
	}

	@Override
	public String register(String receptionPortURI, RegistrationClass rc)
			throws Exception {
		if (receptionPortURI == null || rc == null) {
			throw new RemoteException("register: null parameter.");
		}
		synchronized (registrationLock) {
			if (registered.containsKey(receptionPortURI)) {
				throw new AlreadyRegisteredException("register: already registered " + receptionPortURI);
			}
			registered.put(receptionPortURI, rc);
		}

		// Gossip: propager l'enregistrement aux voisins
		Map<String, Serializable> payload = new HashMap<>();
		payload.put("clientURI", receptionPortURI);
		payload.put("registrationClass", rc);
		gossipEvent(GossipPayloadType.REGISTER, payload);

		return (rc == RegistrationClass.FREE) ? publishingInboundURI : privilegedInboundURI;
	}

	@Override
	public String modifyServiceClass(String receptionPortURI, RegistrationClass rc)
			throws Exception {
		synchronized (registrationLock) {
			if (!registered.containsKey(receptionPortURI)) {
				throw new UnknownClientException("modifyServiceClass: unknown client " + receptionPortURI);
			}
			registered.put(receptionPortURI, rc);
		}

		// Gossip: propager la modification de classe de service
		Map<String, Serializable> payload = new HashMap<>();
		payload.put("clientURI", receptionPortURI);
		payload.put("registrationClass", rc);
		gossipEvent(GossipPayloadType.REGISTER, payload);

		return (rc == RegistrationClass.FREE) ? publishingInboundURI : privilegedInboundURI;
	}

	@Override
	public void unregister(String receptionPortURI) throws Exception {
		synchronized (registrationLock) {
			if (!registered.containsKey(receptionPortURI)) {
				throw new UnknownIdentifierException("unregister: unknown identifier " + receptionPortURI);
			}
			registered.remove(receptionPortURI);
		}

		for (Map<String, MessageFilterI> subs : subscriptions.values()) {
			subs.remove(receptionPortURI);
		}

		ReceivingOutbound rop = receivingOutboundPorts.remove(receptionPortURI);
		if (rop != null) {
			try {
				rop.doDisconnection();
				rop.unpublishPort();
			} catch (Exception e) {
				throw new RemoteException("unregister: error during disconnection.", e);
			}
		}

		List<String> destroyedChannels = new ArrayList<>();
		synchronized (channelLock) {
			Set<String> created = createdChannelsByClient.remove(receptionPortURI);
			if (created != null) {
				for (String ch : created) {
					channels.remove(ch);
					channelCreators.remove(ch);
					channelAuthorisations.remove(ch);
					subscriptions.remove(ch);
					destroyedChannels.add(ch);
				}
			}
		}

		// Gossip: propager le desenregistrement
		Map<String, Serializable> payload = new HashMap<>();
		payload.put("clientURI", receptionPortURI);
		gossipEvent(GossipPayloadType.UNREGISTER, payload);

		// Gossip: propager la destruction des canaux crees par ce client
		for (String ch : destroyedChannels) {
			Map<String, Serializable> chPayload = new HashMap<>();
			chPayload.put("channel", ch);
			chPayload.put("creator", receptionPortURI);
			gossipEvent(GossipPayloadType.DESTROY_CHANNEL, chPayload);
		}
	}

	// =========================================================================
	// Channel queries
	// =========================================================================

	@Override
	public boolean channelExist(String channel) throws Exception {
		return channels.contains(channel);
	}

	@Override
	public boolean channelAuthorised(String receptionPortURI, String channel) throws Exception {
		if (!channels.contains(channel))
			return false;
		String regex = channelAuthorisations.get(channel);
		if (regex == null)
			return true;
		return receptionPortURI.matches(regex);
	}

	@Override
	public boolean hasCreatedChannel(String receptionPortURI, String channel) throws Exception {
		if (!channels.contains(channel))
			return false;
		return receptionPortURI.equals(channelCreators.get(channel));
	}

	@Override
	public boolean isAuthorisedUser(String channel, String uri) throws Exception {
		return channelAuthorised(uri, channel);
	}

	@Override
	public void modifyAuthorisedUsers(String receptionPortURI, String channel,
			String autorisedUsers) throws Exception {
		synchronized (channelLock) {
			if (!hasCreatedChannel(receptionPortURI, channel)) {
				throw new UnauthorisedClientException("Only the channel creator can modify authorised users.");
			}
			channelAuthorisations.put(channel, autorisedUsers);
		}

		// Gossip: propager la modification des autorisations
		Map<String, Serializable> payload = new HashMap<>();
		payload.put("channel", channel);
		payload.put("authorisedUsers", autorisedUsers);
		gossipEvent(GossipPayloadType.MODIFY_AUTH, payload);
	}

	@Override
	public void removeAuthorisedUsers(String receptionPortURI, String channel,
			String regularExpression) throws Exception {
		synchronized (channelLock) {
			if (!hasCreatedChannel(receptionPortURI, channel)) {
				throw new UnauthorisedClientException("Only the channel creator can remove authorised users.");
			}
			channelAuthorisations.put(channel, ".*");
		}

		// Gossip: propager la reinitialisation des autorisations
		Map<String, Serializable> payload = new HashMap<>();
		payload.put("channel", channel);
		payload.put("authorisedUsers", ".*");
		gossipEvent(GossipPayloadType.MODIFY_AUTH, payload);
	}

	// =========================================================================
	// Subscription management
	// =========================================================================

	@Override
	public boolean subscribed(String receptionPortURI, String channel) throws Exception {
		Map<String, MessageFilterI> subs = subscriptions.get(channel);
		return subs != null && subs.containsKey(receptionPortURI);
	}

	@Override
	public void subscribe(String receptionPortURI, String channel,
			MessageFilterI filter) throws Exception {
		if (!channels.contains(channel))
			throw new UnknownChannelException("subscribe: unknown channel " + channel);
		if (!registered.containsKey(receptionPortURI))
			throw new UnknownClientException("subscribe: not registered " + receptionPortURI);
		if (!channelAuthorised(receptionPortURI, channel))
			throw new UnauthorisedClientException("subscribe: unauthorised client for channel " + channel);

		subscriptions
				.computeIfAbsent(channel, k -> new ConcurrentHashMap<>())
				.put(receptionPortURI, filter);
	}

	@Override
	public void unsubscribe(String receptionPortURI, String channel) throws Exception {
		if (!channels.contains(channel))
			throw new UnknownChannelException("unsubscribe: unknown channel " + channel);
		Map<String, MessageFilterI> subs = subscriptions.get(channel);
		if (subs == null || !subs.containsKey(receptionPortURI))
			throw new NotSubscribedChannelException("unsubscribe: not subscribed to " + channel);
		subs.remove(receptionPortURI);
	}

	@Override
	public boolean modifyFilter(String receptionPortURI, String channel,
			MessageFilterI filter) throws Exception {
		if (!channels.contains(channel))
			throw new UnknownChannelException("modifyFilter: unknown channel " + channel);
		Map<String, MessageFilterI> subs = subscriptions.get(channel);
		if (subs == null || !subs.containsKey(receptionPortURI))
			throw new NotSubscribedChannelException("modifyFilter: not subscribed to " + channel);
		subs.put(receptionPortURI, filter);
		return true;
	}

	// =========================================================================
	// Synchronous publish
	// =========================================================================

	@Override
	public void publish(String receptionPortURI, String channel, MessageI message) throws Exception {
		if (!channels.contains(channel))
			throw new UnknownChannelException("publish: unknown channel " + channel);
		if (!registered.containsKey(receptionPortURI))
			throw new UnknownClientException("publish: not registered " + receptionPortURI);
		if (!channelAuthorised(receptionPortURI, channel))
			throw new UnauthorisedClientException("publish: unauthorised client for channel " + channel);
		if (message == null)
			return;

		final MessageI[] batch = new MessageI[] { message };
		this.runTask(PUBLICATION_HANDLER_URI, c -> {
			try {
				((Broker) c).propagateMessages(channel, batch);
			} catch (Exception e) {
				e.printStackTrace();
			}
		});

		// Gossip: propager la publication aux courtiers voisins
		Map<String, Serializable> payload = new HashMap<>();
		payload.put("channel", channel);
		payload.put("messages", batch);
		gossipEvent(GossipPayloadType.PUBLISH, payload);
	}

	@Override
	public void publish(String receptionPortURI, String channel,
			ArrayList<MessageI> messages) throws Exception {
		if (!channels.contains(channel))
			throw new UnknownChannelException("publish: unknown channel " + channel);
		if (!registered.containsKey(receptionPortURI))
			throw new UnknownClientException("publish: not registered " + receptionPortURI);
		if (!channelAuthorised(receptionPortURI, channel))
			throw new UnauthorisedClientException("publish: unauthorised client for channel " + channel);
		if (messages == null || messages.isEmpty())
			return;

		final MessageI[] batch = messages.toArray(new MessageI[0]);
		this.runTask(PUBLICATION_HANDLER_URI, c -> {
			try {
				((Broker) c).propagateMessages(channel, batch);
			} catch (Exception e) {
				e.printStackTrace();
			}
		});

		// Gossip: propager la publication aux courtiers voisins
		Map<String, Serializable> payload = new HashMap<>();
		payload.put("channel", channel);
		payload.put("messages", batch);
		gossipEvent(GossipPayloadType.PUBLISH, payload);
	}

	// =========================================================================
	// Asynchronous publish with abnormal-termination notification (§3.6.2)
	// =========================================================================

	@Override
	public void asyncPublishAndNotify(String receptionPortURI, String channel,
			MessageI message, String notificationInboundPortURI) throws Exception {
		if (!channels.contains(channel))
			throw new UnknownChannelException("asyncPublish: unknown channel " + channel);
		if (!registered.containsKey(receptionPortURI))
			throw new UnknownClientException("asyncPublish: not registered " + receptionPortURI);
		if (!channelAuthorised(receptionPortURI, channel))
			throw new UnauthorisedClientException("asyncPublish: unauthorised client for channel " + channel);

		final MessageI[] batch = new MessageI[] { message };
		this.runTask(PUBLICATION_HANDLER_URI, c -> {
			try {
				((Broker) c).propagateMessages(channel, batch);
			} catch (Exception e) {
				sendAbnormalTerminationNotification(notificationInboundPortURI, e);
			}
		});

		Map<String, Serializable> payload = new HashMap<>();
		payload.put("channel", channel);
		payload.put("messages", batch);
		gossipEvent(GossipPayloadType.PUBLISH, payload);
	}

	@Override
	public void asyncPublishAndNotify(String receptionPortURI, String channel,
			ArrayList<MessageI> messages, String notificationInboundPortURI) throws Exception {
		if (!channels.contains(channel))
			throw new UnknownChannelException("asyncPublish: unknown channel " + channel);
		if (!registered.containsKey(receptionPortURI))
			throw new UnknownClientException("asyncPublish: not registered " + receptionPortURI);
		if (!channelAuthorised(receptionPortURI, channel))
			throw new UnauthorisedClientException("asyncPublish: unauthorised client for channel " + channel);

		final MessageI[] batch = messages.toArray(new MessageI[0]);
		this.runTask(PUBLICATION_HANDLER_URI, c -> {
			try {
				((Broker) c).propagateMessages(channel, batch);
			} catch (Exception e) {
				sendAbnormalTerminationNotification(notificationInboundPortURI, e);
			}
		});

		Map<String, Serializable> payload = new HashMap<>();
		payload.put("channel", channel);
		payload.put("messages", batch);
		gossipEvent(GossipPayloadType.PUBLISH, payload);
	}

	/**
	 * Send an abnormal-termination notification to the client via its
	 * notification inbound port. The outbound port is lazily created
	 * and cached.
	 * 给client发送异步警告
	 */
	private void sendAbnormalTerminationNotification(
			String notificationInboundPortURI, Exception cause) {
		if (notificationInboundPortURI == null) {
			System.err.println("[Broker] async task failed, no notification URI: " + cause.getMessage());
			return;
		}
		try {
			AbnormalTerminationNotificationOutbound np = getOrConnectNotificationOutbound(notificationInboundPortURI);
			np.notifyAbnormalTermination(cause);
		} catch (Exception notifEx) {
			System.err.println("[Broker] failed to send abnormal termination notification: "
					+ notifEx.getMessage());
			cause.printStackTrace();
		}
	}

	private AbnormalTerminationNotificationOutbound getOrConnectNotificationOutbound(
			String notificationInboundPortURI)
			throws Exception {
		AbnormalTerminationNotificationOutbound existing = notificationOutboundPorts.get(notificationInboundPortURI);
		if (existing != null)
			return existing;

		AbnormalTerminationNotificationOutbound np = new AbnormalTerminationNotificationOutbound(this);
		np.publishPort();
		np.doConnection(notificationInboundPortURI,
				AbnormalTerminationNotificationConnector.class.getCanonicalName());
		AbnormalTerminationNotificationOutbound prev = notificationOutboundPorts.putIfAbsent(notificationInboundPortURI,
				np);
		if (prev != null) {
			np.doDisconnection();
			np.unpublishPort();
			return prev;
		}
		return np;
	}

	// =========================================================================
	// Internal message pipeline
	// =========================================================================
	/*
	 * publish → propagateMessages → deliverToOneSubscriber → client.receive()
	 */
	/*
	 * propagateMessages distributed in parallel to multiple subscribers
	 * 并行分发给多个订阅者
	 */
	protected void propagateMessages(String channel, MessageI[] messages) {
		Map<String, MessageFilterI> subs = subscriptions.get(channel);
		if (subs == null || subs.isEmpty())
			return;

		for (Map.Entry<String, MessageFilterI> e : subs.entrySet()) {
			final String clientURI = e.getKey();
			final MessageFilterI filter = e.getValue();

			this.runTask(PROPAGATION_HANDLER_URI, c -> {
				try {
					((Broker) c).deliverToOneSubscriber(channel, clientURI, filter, messages);
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			});
		}
	}

	/*
	 * Select a thread pool based on the client level
	 * 根据client等级选择线程池
	 */
	protected void deliverToOneSubscriber(
			String channel, String clientURI, MessageFilterI filter,
			MessageI[] messages) {

		String deliveryURI = deliveryExecutorURIFor(clientURI);
		this.runTask(deliveryURI, c -> {
			try {
				Broker broker = (Broker) c;
				ReceivingOutbound rop = broker.getOrConnectReceivingOutbound(clientURI);
				for (MessageI m : messages) {
					if (filter == null || filter.match(m)) {
						rop.receive(channel, m);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
	}

	/**
	 * Select the BCM delivery executor service URI based on the client's
	 * service class. PREMIUM clients get the largest pool, FREE the smallest.
	 */
	private String deliveryExecutorURIFor(String clientURI) {
		RegistrationClass rc = registered.get(clientURI);
		if (rc == null)
			return DELIVERY_FREE_HANDLER_URI;
		switch (rc) {
			case PREMIUM:
				return DELIVERY_PREMIUM_HANDLER_URI;
			case STANDARD:
				return DELIVERY_STANDARD_HANDLER_URI;
			default:
				return DELIVERY_FREE_HANDLER_URI;
		}
	}

	protected ReceivingOutbound getOrConnectReceivingOutbound(
			String clientReceptionInboundURI) throws RemoteException {
		ReceivingOutbound existing = receivingOutboundPorts.get(clientReceptionInboundURI);
		if (existing != null)
			return existing;

		try {
			ReceivingOutbound rop = new ReceivingOutbound(this);
			rop.publishPort();
			rop.doConnection(clientReceptionInboundURI,
					ReceivingConnector.class.getCanonicalName());
			ReceivingOutbound prev = receivingOutboundPorts.putIfAbsent(clientReceptionInboundURI, rop);
			if (prev != null) {
				// another thread beat us — clean up and use theirs
				rop.doDisconnection();
				rop.unpublishPort();
				return prev;
			}
			return rop;
		} catch (Exception ex) {
			throw new RemoteException(
					"Cannot connect to client ReceivingInboundPort " + clientReceptionInboundURI, ex);
		}
	}

	// =========================================================================
	// Channel management (synchronized for compound check-then-act)
	// =========================================================================

	@Override
	public boolean channelQuotaReached(String receptionPortURI) throws Exception {
		if (!registered.containsKey(receptionPortURI))
			throw new UnknownIdentifierException("unknown identifier " + receptionPortURI);

		RegistrationClass rc = registered.get(receptionPortURI);
		int quota;
		switch (rc) {
			case STANDARD:
				quota = STANDARD_QUOTA;
				break;
			case PREMIUM:
				quota = PREMIUM_QUOTA;
				break;
			default:
				quota = 0;
		}
		int current = createdChannelsByClient
				.getOrDefault(receptionPortURI, ConcurrentHashMap.newKeySet())
				.size();
		return current >= quota;
	}

	@Override
	public void createChannel(String receptionPortURI, String channel,
			String autorisedUsers) throws Exception {
		if (!registered.containsKey(receptionPortURI))
			throw new UnknownIdentifierException("unknown identifier " + receptionPortURI);

		synchronized (channelLock) {
			if (channels.contains(channel))
				throw new AlreadyExistingChannelException("channel already exists " + channel);

			RegistrationClass rc = registered.get(receptionPortURI);
			if (rc == RegistrationClass.FREE)
				throw new ChannelQuotaExceededException("FREE client cannot create channels");
			if (channelQuotaReached(receptionPortURI))
				throw new ChannelQuotaExceededException("quota exceeded for " + receptionPortURI);

			channels.add(channel);
			channelCreators.put(channel, receptionPortURI);
			channelAuthorisations.put(channel, autorisedUsers == null ? ".*" : autorisedUsers);
			createdChannelsByClient
					.computeIfAbsent(receptionPortURI, k -> ConcurrentHashMap.newKeySet())
					.add(channel);
		}

		// Gossip: propager la creation du canal
		Map<String, Serializable> payload = new HashMap<>();
		payload.put("channel", channel);
		payload.put("creator", receptionPortURI);
		payload.put("authorisedUsers", autorisedUsers == null ? ".*" : autorisedUsers);
		gossipEvent(GossipPayloadType.CREATE_CHANNEL, payload);
	}

	/**
	 * Detruit un canal apres avoir termine la livraison des messages en cours.
	 * Les nouvelles publications sont bloquees (le canal est retire de
	 * {@code channels}),
	 * puis les messages deja soumis aux pools de propagation/livraison sont draines
	 * avant de supprimer les abonnements.
	 */
	@Override
	public void destroyChannel(String receptionPortURI, String channel)
			throws Exception {
		if (!registered.containsKey(receptionPortURI))
			throw new UnknownClientException("unknown client " + receptionPortURI);

		synchronized (channelLock) {
			if (!channels.contains(channel))
				throw new UnknownChannelException("unknown channel " + channel);

			String creator = channelCreators.get(channel);
			if (creator == null || !creator.equals(receptionPortURI))
				throw new UnauthorisedClientException("client is not the creator of " + channel);

			// Etape 1 : retirer le canal pour bloquer toute nouvelle publication
			channels.remove(channel);
			channelCreators.remove(channel);
			channelAuthorisations.remove(channel);

			Set<String> created = createdChannelsByClient.get(receptionPortURI);
			if (created != null)
				created.remove(channel);
		}

		// Etape 2 : laisser le temps aux messages en cours de propagation/livraison
		// de terminer (les taches deja soumises aux executor pools s'executent)
		try {
			Thread.sleep(500);
		} catch (InterruptedException ignored) {
		}

		// Etape 3 : supprimer les abonnements
		subscriptions.remove(channel);

		// Gossip: propager la destruction du canal
		Map<String, Serializable> payload = new HashMap<>();
		payload.put("channel", channel);
		payload.put("creator", receptionPortURI);
		gossipEvent(GossipPayloadType.DESTROY_CHANNEL, payload);
	}

	/**
	 * Detruit un canal immediatement, y compris les messages non encore expedies.
	 */
	@Override
	public void destroyChannelNow(String receptionPortURI, String channel)
			throws Exception {
		if (!registered.containsKey(receptionPortURI))
			throw new UnknownClientException("unknown client " + receptionPortURI);

		synchronized (channelLock) {
			if (!channels.contains(channel))
				throw new UnknownChannelException("unknown channel " + channel);

			String creator = channelCreators.get(channel);
			if (creator == null || !creator.equals(receptionPortURI))
				throw new UnauthorisedClientException("client is not the creator of " + channel);

			channels.remove(channel);
			channelCreators.remove(channel);
			channelAuthorisations.remove(channel);
			subscriptions.remove(channel);

			Set<String> created = createdChannelsByClient.get(receptionPortURI);
			if (created != null)
				created.remove(channel);
		}

		// Gossip: propager la destruction du canal
		Map<String, Serializable> payload = new HashMap<>();
		payload.put("channel", channel);
		payload.put("creator", receptionPortURI);
		gossipEvent(GossipPayloadType.DESTROY_CHANNEL, payload);
	}

	// =========================================================================
	// Gossip protocol implementation (§3.7)
	// =========================================================================

	@Override
	public void receive(GossipMessageI[] gossipMessages) throws Exception {
		this.runTask(GOSSIP_HANDLER_URI, c -> {
			((Broker) c).update(gossipMessages);
		});
	}

	/*
	 * processedGossipURIs.putIfAbsent(uri, timestamp). If the URI already exists,
	 * the message is skipped.
	 * 
	 * Call integrateGossipMessage(gm) to merge the message content into the local
	 * state.
	 * 
	 * Collect all messages that need to be forwarded into the toPropagate list.
	 * 
	 * Finally, call gossipToNeighbors(toPropagate) to propagate the messages to all
	 * neighboring nodes.
	 */
	@Override
	public void update(GossipMessageI[] fromSender) {
		List<GossipMessageI> toPropagate = new ArrayList<>();

		for (GossipMessageI gm : fromSender) {
			String uri = gm.gossipMessageURI();

			// Deduplication atomique
			if (processedGossipURIs.putIfAbsent(uri, gm.timestamp()) != null) {
				continue;
			}

			// Integrer le message dans l'etat local
			integrateGossipMessage(gm);
			toPropagate.add(gm);
		}

		if (!toPropagate.isEmpty()) {
			gossipToNeighbors(toPropagate);
		}
	}

	/**
	 * Integre un message gossip dans l'etat local du courtier.
	 * Les evenements sont traites sans re-validation (deja valides a la source).
	 */
	private void integrateGossipMessage(GossipMessageI gm) {
		GossipMessage msg = (GossipMessage) gm;
		switch (msg.getType()) {
			case PUBLISH: {
				String channel = (String) msg.getPayloadValue("channel");
				MessageI[] messages = (MessageI[]) msg.getPayloadValue("messages");
				// Pas de verif channels.contains() : si CREATE_CHANNEL arrive apres
				// PUBLISH (ordre non garanti), propagateMessages ne trouvera
				// simplement aucun abonne et ne fera rien.
				if (channel != null && messages != null) {
					propagateMessages(channel, messages);
				}
				break;
			}
			case REGISTER: {
				String clientURI = (String) msg.getPayloadValue("clientURI");
				RegistrationClass rc = (RegistrationClass) msg.getPayloadValue("registrationClass");
				if (clientURI != null && rc != null) {
					// put (et non putIfAbsent) pour que modifyServiceClass puisse
					// mettre a jour la classe de service sur les courtiers distants
					registered.put(clientURI, rc);
				}
				break;
			}
			case UNREGISTER: {
				String clientURI = (String) msg.getPayloadValue("clientURI");
				if (clientURI != null) {
					registered.remove(clientURI);
					// Nettoyer les abonnements du client sur ce broker
					for (Map<String, MessageFilterI> subs : subscriptions.values()) {
						subs.remove(clientURI);
					}
					// Nettoyer le port sortant cache
					ReceivingOutbound rop = receivingOutboundPorts.remove(clientURI);
					if (rop != null) {
						try {
							rop.doDisconnection();
						} catch (Throwable ignored) {
						}
						try {
							rop.unpublishPort();
						} catch (Throwable ignored) {
						}
					}
				}
				break;
			}
			case CREATE_CHANNEL: {
				String channel = (String) msg.getPayloadValue("channel");
				String creator = (String) msg.getPayloadValue("creator");
				String auth = (String) msg.getPayloadValue("authorisedUsers");
				if (channel != null) {
					synchronized (channelLock) {
						if (!channels.contains(channel)) {
							channels.add(channel);
							String c = creator != null ? creator : "REMOTE";
							channelCreators.put(channel, c);
							channelAuthorisations.put(channel, auth != null ? auth : ".*");
							createdChannelsByClient
									.computeIfAbsent(c, k -> ConcurrentHashMap.newKeySet())
									.add(channel);
						}
					}
				}
				break;
			}
			case DESTROY_CHANNEL: {
				String channel = (String) msg.getPayloadValue("channel");
				String creator = (String) msg.getPayloadValue("creator");
				if (channel != null) {
					synchronized (channelLock) {
						channels.remove(channel);
						channelCreators.remove(channel);
						channelAuthorisations.remove(channel);
						subscriptions.remove(channel);
						if (creator != null) {
							Set<String> created = createdChannelsByClient.get(creator);
							if (created != null) {
								created.remove(channel);
							}
						}
					}
				}
				break;
			}
			case MODIFY_AUTH: {
				String channel = (String) msg.getPayloadValue("channel");
				String newAuth = (String) msg.getPayloadValue("authorisedUsers");
				if (channel != null && newAuth != null) {
					channelAuthorisations.put(channel, newAuth);
				}
				break;
			}
		}
	}

	/**
	 * Transmet les messages gossip a tous les voisins, en remplacant l'URI
	 * de l'emetteur par celui de ce courtier.
	 */
	private void gossipToNeighbors(List<GossipMessageI> messages) {
		for (Map.Entry<String, GossipSenderOutbound> entry : gossipSenderOutbounds.entrySet()) {
			String neighborURI = entry.getKey();
			GossipSenderOutbound outbound = entry.getValue();

			// Filtrer : ne pas renvoyer un message au voisin qui l'a emis
			List<GossipMessageI> filtered = new ArrayList<>();
			for (GossipMessageI gm : messages) {
				GossipMessage g = (GossipMessage) gm;
				if (!neighborURI.equals(g.getEmitterURI())) {
					filtered.add(gm.copyWithNewEmitterURI(this.gossipReceiverInboundURI));
				}
			}
			if (filtered.isEmpty())
				continue;

			final GossipMessageI[] arr = filtered.toArray(new GossipMessageI[0]);
			this.runTask(GOSSIP_HANDLER_URI, c -> {
				try {
					outbound.send(arr);
				} catch (Exception e) {
					System.err.println("[Broker " + brokerURI
							+ "] gossip send failed to " + neighborURI
							+ ": " + e.getMessage());
				}
			});
		}
	}

	/**
	 * Cree un message gossip pour un evenement local et le propage aux voisins.
	 * Ne fait rien si aucun voisin n'est connecte (mode mono-courtier).
	 */
	private void gossipEvent(GossipPayloadType type, Map<String, Serializable> payload) {
		if (gossipSenderOutbounds.isEmpty())
			return;

		String uri = brokerURI + "-gossip-" + UUID.randomUUID();
		// emitterURI = notre gossip inbound URI, pour que le voisin puisse
		// eviter de nous renvoyer ce message (filtrage par emitter)
		GossipMessage gm = new GossipMessage(uri, gossipReceiverInboundURI, type, payload);
		processedGossipURIs.put(uri, gm.timestamp());
		gossipToNeighbors(Collections.singletonList(gm));
	}
}
