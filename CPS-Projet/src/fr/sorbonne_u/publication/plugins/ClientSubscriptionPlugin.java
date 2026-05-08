package fr.sorbonne_u.publication.plugins;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.cps.pubsub.exceptions.NotSubscribedChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnauthorisedClientException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownClientException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.messages.MessageFilter;
import fr.sorbonne_u.publication.plugins.interfaces.ClientSubscriptionI;
import fr.sorbonne_u.publication.ports.inbound.ReceivingInbound;

/**
 * Subscription plugin managing channel subscriptions, message reception,
 * and advanced reception modes ({@code waitForNextMessage},
 * {@code getNextMessage}).
 *
 * <p>
 * When a message arrives on a channel, the plugin first checks whether any
 * thread is waiting for a message on that channel (via
 * {@code waitForNextMessage}
 * or {@code getNextMessage}). Waiting requests are served in FIFO order.
 * Only when no waiter exists does the plugin fall through to the normal
 * {@code receive()} path (logging + domain hook).
 * </p>
 *
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class ClientSubscriptionPlugin
		extends AbstractClientPlugin
		implements ClientSubscriptionI {

	private static final long serialVersionUID = 1L;

	protected ReceivingInbound receivingInboundPort;

	/**
	 * Per-channel FIFO queue of pending message waiters.
	 * Each entry is a {@link CompletableFuture} that will be completed
	 * with the next message arriving on the corresponding channel.
	 */
	private final ConcurrentHashMap<String, ConcurrentLinkedQueue<CompletableFuture<MessageI>>> pendingWaiters = new ConcurrentHashMap<>();

	// =========================================================================
	// Plugin lifecycle
	// =========================================================================

	@Override
	public void installOn(ComponentI owner) throws Exception {
		super.installOn(owner);

		this.addOfferedInterface(ReceivingCI.class);

		this.receivingInboundPort = new ReceivingInbound(this.receivingInboundURI, owner);
		this.receivingInboundPort.publishPort();
	}

	@Override
	public void initialise() throws Exception {
		super.initialise();
	}

	@Override
	public void uninstall() throws Exception {
		// Cancel all pending waiters
		for (ConcurrentLinkedQueue<CompletableFuture<MessageI>> q : pendingWaiters.values()) {
			CompletableFuture<MessageI> f;
			while ((f = q.poll()) != null) {
				f.cancel(false);
			}
		}
		pendingWaiters.clear();

		try {
			this.receivingInboundPort.unpublishPort();
		} catch (Exception ignored) {
		}
		this.removeOfferedInterface(ReceivingCI.class);
		super.uninstall();
	}

	// =========================================================================
	// Subscription management (delegates to broker via registration port)
	// =========================================================================

	@Override
	public boolean channelExist(String channel) throws Exception {
		return this.owner()
				.getRegistrationPlugin()
				.getRegistrationOutbound()
				.channelExist(channel);
	}

	@Override
	public boolean channelAuthorised(String channel) throws Exception {
		return this.owner()
				.getRegistrationPlugin()
				.getRegistrationOutbound()
				.channelAuthorised(this.receivingInboundURI, channel);
	}

	@Override
	public boolean subscribed(String channel) throws Exception {
		return this.owner()
				.getRegistrationPlugin()
				.getRegistrationOutbound()
				.subscribed(this.receivingInboundURI, channel);
	}

	@Override
	public void subscribe(String channel, MessageFilterI filter) throws Exception {
		MessageFilterI f = (filter != null) ? filter : new MessageFilter(null, null, null);
		this.owner()
				.getRegistrationPlugin()
				.getRegistrationOutbound()
				.subscribe(this.receivingInboundURI, channel, f);
	}

	@Override
	public void unsubscribe(String channel) throws Exception {
		this.owner()
				.getRegistrationPlugin()
				.getRegistrationOutbound()
				.unsubscribe(this.receivingInboundURI, channel);
	}

	@Override
	public void modifyFilter(String channel, MessageFilterI filter) throws Exception {
		this.owner()
				.getRegistrationPlugin()
				.getRegistrationOutbound()
				.modifyFilter(this.receivingInboundURI, channel, filter);
	}

	// =========================================================================
	// Message reception — waiter-first dispatch
	// =========================================================================

	/**
	 * Entry point for a single incoming message.
	 *
	 * <p>
	 * If a pending waiter ({@code waitForNextMessage} or {@code getNextMessage})
	 * exists for this channel, the message is dispatched directly to it (FIFO)
	 * and no further processing occurs. Otherwise the message follows the
	 * normal path: console log + domain hook
	 * ({@link Client#onMessageReceived}).
	 * </p>
	 */
	@Override
	public void receive(String channel, MessageI message) {
		// 1. Try waiter-first dispatch
		ConcurrentLinkedQueue<CompletableFuture<MessageI>> queue = pendingWaiters.get(channel);
		if (queue != null) {
			CompletableFuture<MessageI> waiter;
			while ((waiter = queue.poll()) != null) {
				if (waiter.complete(message)) {
					System.out.println("[" + this.receivingInboundURI
							+ "] dispatched to waiter: channel=" + channel
							+ ", message=" + message);
					return; // consumed — skip normal path
				}
				// else: cancelled future, try next
			}
		}

		// 2. Normal path: log + domain hook
		System.out.println("[" + this.receivingInboundURI + "] receive: channel="
				+ channel + ", message=" + message);
		this.owner().logMessage("RECEIVED on " + channel + " : " + message);
		this.owner().onMessageReceived(channel, message);
	}

	@Override
	public void receive(String channel, MessageI[] messages) {
		if (messages == null)
			return;
		for (MessageI m : messages) {
			receive(channel, m);
		}
	}

	// =========================================================================
	// Advanced reception (§3.5.3)
	// =========================================================================

	/**
	 * Block the calling thread until a message is received on {@code channel}.
	 *
	 * <p>
	 * The request is queued in FIFO order. When a message arrives and this
	 * request is at the head of the queue, the message is returned directly
	 * (bypassing the normal {@code receive()} path).
	 * </p>
	 */
	@Override
	public MessageI waitForNextMessage(String channel) throws Exception {
		CompletableFuture<MessageI> future = enqueueWaiter(channel);
		return future.get(); // blocks indefinitely
	}

	/**
	 * Block the calling thread for at most {@code d} waiting for a message.
	 *
	 * @return the message, or {@code null} if the timeout expired.
	 */
	@Override
	public MessageI waitForNextMessage(String channel, Duration d) throws Exception {
		CompletableFuture<MessageI> future = enqueueWaiter(channel);
		try {
			return future.get(d.toMillis(), TimeUnit.MILLISECONDS);
		} catch (TimeoutException e) {
			// Remove the unfulfilled waiter so it doesn't leak
			future.cancel(false);
			ConcurrentLinkedQueue<CompletableFuture<MessageI>> queue = pendingWaiters.get(channel);
			if (queue != null) {
				queue.remove(future);
			}
			return null;
		}
	}

	/**
	 * Return a {@link Future} that will be completed with the next message
	 * on {@code channel}. The calling thread is <em>not</em> blocked.
	 *
	 * <p>
	 * Multiple calls create multiple futures, each consuming one message
	 * in FIFO order.
	 * </p>
	 */
	@Override
	public Future<MessageI> getNextMessage(String channel) throws Exception {
		return enqueueWaiter(channel);
	}

	// =========================================================================
	// Internal helpers
	// =========================================================================

	/**
	 * Verifie les preconditions puis cree un {@link CompletableFuture}
	 * ajoute a la file d'attente du canal.
	 */
	private CompletableFuture<MessageI> enqueueWaiter(String channel) throws Exception {
		if (!this.owner().getRegistrationPlugin().registered())
			throw new UnknownClientException("client not registered");
		if (!channelExist(channel))
			throw new UnknownChannelException("unknown channel " + channel);
		if (!channelAuthorised(channel))
			throw new UnauthorisedClientException("not authorised for " + channel);
		if (!subscribed(channel))
			throw new NotSubscribedChannelException("not subscribed to " + channel);

		CompletableFuture<MessageI> future = new CompletableFuture<>();
		pendingWaiters
				.computeIfAbsent(channel, k -> new ConcurrentLinkedQueue<>())
				.add(future);
		return future;
	}
}
