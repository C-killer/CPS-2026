package fr.sorbonne_u.publication;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.cps.pubsub.exceptions.AlreadyExistingChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.AlreadyRegisteredException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownIdentifierException;
import fr.sorbonne_u.cps.pubsub.interfaces.ChannelQuotaExceededException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.publication.connectors.AbnormalTerminationNotificationConnector;
import fr.sorbonne_u.publication.connectors.ReceivingConnector;
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
 * Thread-safety: all mutable shared state uses {@link ConcurrentHashMap}
 * and compound check-then-act operations (register, create/destroy channel,
 * unregister) are {@code synchronized} on {@code this}.
 * </p>
 *
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class Broker extends AbstractComponent
		implements RegistrationImplI, PrivilegedClientImpli {

	protected final RegistrationInbound registrationInboundPort;
	protected final PublishingInbound publishingInboundPort;
	protected final PrivilegedClientInbound privilegedClientInboundPort;

	protected static final String REGISTRATION_INBOUND_URI = "broker-registration";
	protected static final String PUBLISHING_INBOUND_URI = "broker-publishing";
	protected static final String PRIVILEGED_INBOUND_URI = "broker-privileged";

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

	// --- BCM executor service URIs (§3.6.3) ---线程池
	public static final String PUBLICATION_HANDLER_URI = "publication-pool";
	public static final String PROPAGATION_HANDLER_URI = "propagation-pool";
	public static final String DELIVERY_PREMIUM_HANDLER_URI = "delivery-premium-pool";
	public static final String DELIVERY_STANDARD_HANDLER_URI = "delivery-standard-pool";
	public static final String DELIVERY_FREE_HANDLER_URI = "delivery-free-pool";

	// =========================================================================
	// Construction / lifecycle
	// =========================================================================

	protected Broker(int nbChannels) throws Exception {
		super(4, 0);

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

		for (int i = 0; i < nbChannels; i++) {
			String c = "channel" + i;
			channels.add(c);
			channelCreators.put(c, "SYSTEM");
			channelAuthorisations.put(c, ".*");
		}

		this.registrationInboundPort = new RegistrationInbound(REGISTRATION_INBOUND_URI, this);
		this.registrationInboundPort.publishPort();

		this.publishingInboundPort = new PublishingInbound(PUBLISHING_INBOUND_URI, this);
		this.publishingInboundPort.publishPort();

		this.privilegedClientInboundPort = new PrivilegedClientInbound(PRIVILEGED_INBOUND_URI, this);
		this.privilegedClientInboundPort.publishPort();
	}

	@Override
	public synchronized void finalise() throws Exception {
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

		// BCM manages executor service shutdown automatically
		super.finalise();
	}

	@Override
	public synchronized void shutdown() throws ComponentShutdownException {
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
		super.shutdown();
	}

	public static String registrationPortURI() {
		return REGISTRATION_INBOUND_URI;
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
	public synchronized String register(String receptionPortURI, RegistrationClass rc)
			throws Exception {
		if (receptionPortURI == null || rc == null) {
			throw new RemoteException("register: null parameter.");
		}
		if (registered.containsKey(receptionPortURI)) {
			throw new AlreadyRegisteredException("register: already registered " + receptionPortURI);
		}
		registered.put(receptionPortURI, rc);
		return (rc == RegistrationClass.FREE) ? PUBLISHING_INBOUND_URI : PRIVILEGED_INBOUND_URI;
	}

	@Override
	public synchronized String modifyServiceClass(String receptionPortURI, RegistrationClass rc)
			throws Exception {
		if (!registered.containsKey(receptionPortURI)) {
			throw new AlreadyRegisteredException("modifyServiceClass: unknown identifier " + receptionPortURI);
		}
		registered.put(receptionPortURI, rc);
		return (rc == RegistrationClass.FREE) ? PUBLISHING_INBOUND_URI : PRIVILEGED_INBOUND_URI;
	}

	@Override
	public synchronized void unregister(String receptionPortURI) throws Exception {
		if (!registered.containsKey(receptionPortURI)) {
			throw new UnknownIdentifierException("unregister: unknown identifier " + receptionPortURI);
		}
		registered.remove(receptionPortURI);

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

		Set<String> created = createdChannelsByClient.remove(receptionPortURI);
		if (created != null) {
			for (String ch : created) {
				channels.remove(ch);
				channelCreators.remove(ch);
				channelAuthorisations.remove(ch);
				subscriptions.remove(ch);
			}
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
		if (!hasCreatedChannel(receptionPortURI, channel)) {
			throw new Exception("Only the channel creator can modify authorised users.");
		}
		channelAuthorisations.put(channel, autorisedUsers);
	}

	@Override
	public void removeAuthorisedUsers(String receptionPortURI, String channel,
			String regularExpression) throws Exception {
		if (!hasCreatedChannel(receptionPortURI, channel)) {
			throw new Exception("Only the channel creator can remove authorised users.");
		}
		channelAuthorisations.put(channel, ".*");
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
	public synchronized void subscribe(String receptionPortURI, String channel,
			MessageFilterI filter) throws Exception {
		if (!channels.contains(channel))
			throw new RemoteException("subscribe: unknown channel " + channel);
		if (!registered.containsKey(receptionPortURI))
			throw new RemoteException("subscribe: not registered " + receptionPortURI);
		if (!channelAuthorised(receptionPortURI, channel))
			throw new Exception("subscribe: unauthorised client for channel " + channel);

		subscriptions
				.computeIfAbsent(channel, k -> new ConcurrentHashMap<>())
				.put(receptionPortURI, filter);
	}

	@Override
	public void unsubscribe(String receptionPortURI, String channel) throws Exception {
		if (!channels.contains(channel))
			throw new RemoteException("unsubscribe: unknown channel " + channel);
		Map<String, MessageFilterI> subs = subscriptions.get(channel);
		if (subs != null)
			subs.remove(receptionPortURI);
	}

	@Override
	public boolean modifyFilter(String receptionPortURI, String channel,
			MessageFilterI filter) throws Exception {
		if (!channels.contains(channel))
			throw new RemoteException("modifyFilter: unknown channel " + channel);
		Map<String, MessageFilterI> subs = subscriptions.get(channel);
		if (subs == null || !subs.containsKey(receptionPortURI))
			return false;
		subs.put(receptionPortURI, filter);
		return true;
	}

	// =========================================================================
	// Synchronous publish
	// =========================================================================

	@Override
	public void publish(String receptionPortURI, String channel, MessageI message) throws Exception {
		if (!channels.contains(channel))
			throw new RemoteException("publish: unknown channel " + channel);
		if (!registered.containsKey(receptionPortURI))
			throw new RemoteException("publish: not registered " + receptionPortURI);
		if (!channelAuthorised(receptionPortURI, channel))
			throw new Exception("publish: unauthorised client for channel " + channel);
		if (message == null)
			return;

		this.runTask(PUBLICATION_HANDLER_URI, c -> {
			try {
				((Broker) c).propagateMessages(channel, new MessageI[] { message });
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
	}

	@Override
	public void publish(String receptionPortURI, String channel,
			ArrayList<MessageI> messages) throws Exception {
		if (!channels.contains(channel))
			throw new RemoteException("publish: unknown channel " + channel);
		if (!registered.containsKey(receptionPortURI))
			throw new RemoteException("publish: not registered " + receptionPortURI);
		if (!channelAuthorised(receptionPortURI, channel))
			throw new Exception("publish: unauthorised client for channel " + channel);
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
	}

	// =========================================================================
	// Asynchronous publish with abnormal-termination notification (§3.6.2)
	// =========================================================================

	@Override
	public void asyncPublishAndNotify(String receptionPortURI, String channel,
			MessageI message, String notificationInboundPortURI) throws Exception {
		if (!channels.contains(channel))
			throw new UnknownChannelException("asyncPublish: unknown channel " + channel);
		if (!channelAuthorised(receptionPortURI, channel))
			throw new Exception("asyncPublish: unauthorised client for channel " + channel);

		this.runTask(PUBLICATION_HANDLER_URI, c -> {
			try {
				((Broker) c).propagateMessages(channel, new MessageI[] { message });
			} catch (Exception e) {
				sendAbnormalTerminationNotification(notificationInboundPortURI, e);
			}
		});
	}

	@Override
	public void asyncPublishAndNotify(String receptionPortURI, String channel,
			ArrayList<MessageI> messages, String notificationInboundPortURI) throws Exception {
		if (!channels.contains(channel))
			throw new UnknownChannelException("asyncPublish: unknown channel " + channel);
		if (!channelAuthorised(receptionPortURI, channel))
			throw new Exception("asyncPublish: unauthorised client for channel " + channel);

		final MessageI[] batch = messages.toArray(new MessageI[0]);
		this.runTask(PUBLICATION_HANDLER_URI, c -> {
			try {
				((Broker) c).propagateMessages(channel, batch);
			} catch (Exception e) {
				sendAbnormalTerminationNotification(notificationInboundPortURI, e);
			}
		});
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

	private synchronized AbnormalTerminationNotificationOutbound getOrConnectNotificationOutbound(
			String notificationInboundPortURI)
			throws Exception {
		AbnormalTerminationNotificationOutbound np = notificationOutboundPorts.get(notificationInboundPortURI);
		if (np != null)
			return np;

		np = new AbnormalTerminationNotificationOutbound(this);
		np.publishPort();
		np.doConnection(notificationInboundPortURI,
				AbnormalTerminationNotificationConnector.class.getCanonicalName());
		notificationOutboundPorts.put(notificationInboundPortURI, np);
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
			// CPS pattern: use a temporary thread for the blocking
			// outbound call (rop.receive), so the delivery-pool thread
			// is freed immediately and can serve other deliveries.
			final Broker broker = (Broker) c;
			new Thread(() -> {
				try {
					ReceivingOutbound rop = broker.getOrConnectReceivingOutbound(clientURI);
					for (MessageI m : messages) {
						if (filter == null || filter.match(m)) {
							// blocking outbound call on temporary thread
							rop.receive(channel, m);
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}).start();
			// delivery-pool thread released here
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

	protected synchronized ReceivingOutbound getOrConnectReceivingOutbound(
			String clientReceptionInboundURI) throws RemoteException {
		ReceivingOutbound rop = receivingOutboundPorts.get(clientReceptionInboundURI);
		if (rop != null)
			return rop;

		try {
			rop = new ReceivingOutbound(this);
			rop.publishPort();
			rop.doConnection(clientReceptionInboundURI,
					ReceivingConnector.class.getCanonicalName());
			receivingOutboundPorts.put(clientReceptionInboundURI, rop);
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
	public synchronized void createChannel(String receptionPortURI, String channel,
			String autorisedUsers) throws Exception {
		if (!registered.containsKey(receptionPortURI))
			throw new UnknownIdentifierException("unknown identifier " + receptionPortURI);
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

	@Override
	public synchronized void destroyChannel(String receptionPortURI, String channel)
			throws Exception {
		if (!registered.containsKey(receptionPortURI))
			throw new UnknownIdentifierException("unknown identifier " + receptionPortURI);
		if (!channels.contains(channel))
			throw new UnknownChannelException("unknown channel " + channel);

		String creator = channelCreators.get(channel);
		if (creator == null || !creator.equals(receptionPortURI))
			throw new UnknownIdentifierException("client is not the creator of " + channel);

		channels.remove(channel);
		channelCreators.remove(channel);
		channelAuthorisations.remove(channel);
		subscriptions.remove(channel);

		Set<String> created = createdChannelsByClient.get(receptionPortURI);
		if (created != null)
			created.remove(channel);
	}

	@Override
	public synchronized void destroyChannelNow(String receptionPortURI, String channel)
			throws Exception {
		destroyChannel(receptionPortURI, channel);
	}
}
