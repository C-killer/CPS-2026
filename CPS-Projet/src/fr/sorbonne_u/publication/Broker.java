package fr.sorbonne_u.publication;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.cps.pubsub.exceptions.AlreadyExistingChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.AlreadyRegisteredException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownIdentifierException;
import fr.sorbonne_u.cps.pubsub.interfaces.ChannelQuotaExceededException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.publication.connectors.ReceivingConnector;
import fr.sorbonne_u.publication.implementations.PrivilegedClientImpli;
import fr.sorbonne_u.publication.implementations.RegistrationImplI;
import fr.sorbonne_u.publication.ports.inbound.PrivilegedClientInbound;
import fr.sorbonne_u.publication.ports.inbound.PublishingInbound;
import fr.sorbonne_u.publication.ports.inbound.RegistrationInbound;
import fr.sorbonne_u.publication.ports.outbound.ReceivingOutbound;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;

public class Broker extends AbstractComponent
		implements RegistrationImplI, PrivilegedClientImpli {

	protected final RegistrationInbound registrationInboundPort;
	protected final PublishingInbound publishingInboundPort;
	protected final PrivilegedClientInbound privilegedClientInboundPort;

	protected static final String REGISTRATION_INBOUND_URI = "broker-registration";
	protected static final String PUBLISHING_INBOUND_URI = "broker-publishing";
	protected static final String PRIVILEGED_INBOUND_URI = "broker-privileged";

	protected final Set<String> channels = ConcurrentHashMap.newKeySet();
	protected final ConcurrentMap<String, String> channelCreators = new ConcurrentHashMap<>();
	// 新增：用于存储频道授权的正则表达式
	protected final ConcurrentMap<String, String> channelAuthorisations = new ConcurrentHashMap<>();
	protected final ConcurrentMap<String, Set<String>> createdChannelsByClient = new ConcurrentHashMap<>();
	protected final ConcurrentMap<String, RegistrationClass> registered = new ConcurrentHashMap<>();
	protected final ConcurrentMap<String, Map<String, MessageFilterI>> subscriptions = new ConcurrentHashMap<>();
	protected final ConcurrentMap<String, ReceivingOutbound> receivingOutboundPorts = new ConcurrentHashMap<>();

	protected static final int STANDARD_QUOTA = 3;
	protected static final int PREMIUM_QUOTA = 10;

	// semaine 6
	protected final ExecutorService publicationExecutor;
	protected final ExecutorService propagationExecutor;
	protected final ExecutorService deliveryExecutor;

	public Broker(int nbChannels) throws Exception {
		super(4, 0);

		this.publicationExecutor = Executors.newFixedThreadPool(2);
		this.propagationExecutor = Executors.newFixedThreadPool(4);
		this.deliveryExecutor = Executors.newFixedThreadPool(4);

		for (int i = 0; i < nbChannels; i++) {
			String c = "channel" + i;
			channels.add(c);
			channelCreators.put(c, "SYSTEM");
			channelAuthorisations.put(c, ".*"); // 默认系统频道允许所有人访问
		}

		this.registrationInboundPort = new RegistrationInbound(REGISTRATION_INBOUND_URI, this);
		this.registrationInboundPort.publishPort();

		this.publishingInboundPort = new PublishingInbound(PUBLISHING_INBOUND_URI, this);
		this.publishingInboundPort.publishPort();

		this.privilegedClientInboundPort = new PrivilegedClientInbound(PRIVILEGED_INBOUND_URI, this);
		this.privilegedClientInboundPort.publishPort();
	}

	public static String registrationPortURI() {
		return REGISTRATION_INBOUND_URI;
	}

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
		if (registered.containsKey(receptionPortURI)) {
			throw new AlreadyRegisteredException("register: already registered " + receptionPortURI);
		}

		registered.put(receptionPortURI, rc);

		if (rc == RegistrationClass.FREE) {
			return PUBLISHING_INBOUND_URI;
		} else {
			return PRIVILEGED_INBOUND_URI;
		}
	}

	@Override
	public String modifyServiceClass(String receptionPortURI, RegistrationClass rc)
			throws Exception {
		if (!registered.containsKey(receptionPortURI)) {
			throw new AlreadyRegisteredException("modifyServiceClass: unknown identifier " + receptionPortURI);
		}

		registered.put(receptionPortURI, rc);

		if (rc == RegistrationClass.FREE) {
			return PUBLISHING_INBOUND_URI;
		} else {
			return PRIVILEGED_INBOUND_URI;
		}
	}

	@Override
	public void unregister(String receptionPortURI) throws Exception {
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
				channelAuthorisations.remove(ch); // 清理授权表
				subscriptions.remove(ch);
			}
		}
	}

	@Override
	public boolean channelExist(String channel) throws Exception {
		return channels.contains(channel);
	}

	// ------------------------------------------------------------------------
	// 新增：鉴权验证逻辑 (RegistrationImplI / PrivilegedClientImpli)
	// ------------------------------------------------------------------------
	@Override
	public boolean channelAuthorised(String receptionPortURI, String channel) throws Exception {
		if (!channels.contains(channel))
			return false;
		String regex = channelAuthorisations.get(channel);
		if (regex == null)
			return true; // 默认放行
		return receptionPortURI.matches(regex);
	}

	@Override
	public boolean hasCreatedChannel(String receptionPortURI, String channel) throws Exception {
		if (!channels.contains(channel))
			return false;
		String creator = channelCreators.get(channel);
		return receptionPortURI.equals(creator);
	}

	@Override
	public boolean isAuthorisedUser(String channel, String uri) throws Exception {
		return channelAuthorised(uri, channel);
	}

	@Override
	public void modifyAuthorisedUsers(String receptionPortURI, String channel, String autorisedUsers) throws Exception {
		if (!hasCreatedChannel(receptionPortURI, channel)) {
			throw new Exception("Error: Only the channel creator can modify authorised users.");
		}
		channelAuthorisations.put(channel, autorisedUsers);
	}

	@Override
	public void removeAuthorisedUsers(String receptionPortURI, String channel, String regularExpression)
			throws Exception {
		if (!hasCreatedChannel(receptionPortURI, channel)) {
			throw new Exception("Error: Only the channel creator can remove authorised users.");
		}
		// 重置为允许所有人
		channelAuthorisations.put(channel, ".*");
	}

	@Override
	public boolean subscribed(String receptionPortURI, String channel) throws Exception {
		Map<String, MessageFilterI> subs = subscriptions.get(channel);
		return subs != null && subs.containsKey(receptionPortURI);
	}

	@Override
	public void subscribe(String receptionPortURI, String channel, MessageFilterI filter) throws Exception {
		if (!channels.contains(channel)) {
			throw new RemoteException("subscribe: unknown channel " + channel);
		}
		if (!registered.containsKey(receptionPortURI)) {
			throw new RemoteException("subscribe: not registered " + receptionPortURI);
		}
		// 校验授权
		if (!channelAuthorised(receptionPortURI, channel)) {
			throw new Exception("subscribe: unauthorised client for channel " + channel);
		}

		subscriptions
				.computeIfAbsent(channel, k -> new ConcurrentHashMap<>())
				.put(receptionPortURI, filter);
	}

	@Override
	public void unsubscribe(String receptionPortURI, String channel) throws Exception {
		if (!channels.contains(channel)) {
			throw new RemoteException("unsubscribe: unknown channel " + channel);
		}
		Map<String, MessageFilterI> subs = subscriptions.get(channel);
		if (subs != null) {
			subs.remove(receptionPortURI);
		}
	}

	@Override
	public boolean modifyFilter(String receptionPortURI, String channel, MessageFilterI filter) throws Exception {
		if (!channels.contains(channel)) {
			throw new RemoteException("modifyFilter: unknown channel " + channel);
		}
		Map<String, MessageFilterI> subs = subscriptions.get(channel);
		if (subs == null || !subs.containsKey(receptionPortURI)) {
			return false;
		}
		subs.put(receptionPortURI, filter);
		return true;
	}

	@Override
	public void publish(String receptionPortURI, String channel, MessageI message) throws Exception {
		if (!channels.contains(channel)) {
			throw new RemoteException("publish: unknown channel " + channel);
		}
		if (!registered.containsKey(receptionPortURI)) {
			throw new RemoteException("publish: not registered " + receptionPortURI);
		}
		if (!channelAuthorised(receptionPortURI, channel)) {
			throw new Exception("publish: unauthorised client for channel " + channel);
		}
		if (message == null) {
			return;
		}

		this.publicationExecutor.submit(() -> {
			try {
				this.propagateMessages(channel, new MessageI[] { message });
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
	}

	@Override
	public void publish(String receptionPortURI, String channel, ArrayList<MessageI> messages) throws Exception {
		if (!channels.contains(channel)) {
			throw new RemoteException("publish: unknown channel " + channel);
		}
		if (!registered.containsKey(receptionPortURI)) {
			throw new RemoteException("publish: not registered " + receptionPortURI);
		}
		if (!channelAuthorised(receptionPortURI, channel)) {
			throw new Exception("publish: unauthorised client for channel " + channel);
		}
		if (messages == null || messages.isEmpty()) {
			return;
		}

		final MessageI[] batch = messages.toArray(new MessageI[0]);

		this.publicationExecutor.submit(() -> {
			try {
				this.propagateMessages(channel, batch);
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
	}

	// 新增：支持异步发布并通知
	@Override
	public void asyncPublishAndNotify(String receptionPortURI, String channel, MessageI message,
			String notificationInboundPortURI) throws Exception {
		if (!channels.contains(channel)) {
			throw new UnknownChannelException("asyncPublish: unknown channel " + channel);
		}
		if (!channelAuthorised(receptionPortURI, channel)) {
			throw new Exception("asyncPublish: unauthorised client for channel " + channel);
		}

		this.publicationExecutor.submit(() -> {
			try {
				this.propagateMessages(channel, new MessageI[] { message });
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
	}

	@Override
	public void asyncPublishAndNotify(String receptionPortURI, String channel, ArrayList<MessageI> messages,
			String notificationInboundPortURI) throws Exception {
		if (!channels.contains(channel)) {
			throw new UnknownChannelException("asyncPublish: unknown channel " + channel);
		}
		if (!channelAuthorised(receptionPortURI, channel)) {
			throw new Exception("asyncPublish: unauthorised client for channel " + channel);
		}

		final MessageI[] batch = messages.toArray(new MessageI[0]);
		this.publicationExecutor.submit(() -> {
			try {
				this.propagateMessages(channel, batch);
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
	}

	protected void propagateMessages(String channel, MessageI[] messages) {
		Map<String, MessageFilterI> subs = subscriptions.get(channel);
		if (subs == null || subs.isEmpty()) {
			return;
		}

		for (Map.Entry<String, MessageFilterI> e : subs.entrySet()) {
			final String clientReceptionInboundURI = e.getKey();
			final MessageFilterI filter = e.getValue();

			this.propagationExecutor.submit(() -> {
				try {
					this.deliverToOneSubscriber(channel, clientReceptionInboundURI, filter, messages);
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			});
		}
	}

	protected void deliverToOneSubscriber(
			String channel,
			String clientReceptionInboundURI,
			MessageFilterI filter,
			MessageI[] messages) {

		this.deliveryExecutor.submit(() -> {
			try {
				ReceivingOutbound rop = getOrConnectReceivingOutbound(clientReceptionInboundURI);

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

	protected synchronized ReceivingOutbound getOrConnectReceivingOutbound(String clientReceptionInboundURI)
			throws RemoteException {

		ReceivingOutbound rop = receivingOutboundPorts.get(clientReceptionInboundURI);
		if (rop != null) {
			return rop;
		}

		try {
			rop = new ReceivingOutbound(this);
			rop.publishPort();
			rop.doConnection(
					clientReceptionInboundURI,
					ReceivingConnector.class.getCanonicalName());
			receivingOutboundPorts.put(clientReceptionInboundURI, rop);
			return rop;
		} catch (Exception ex) {
			throw new RemoteException("Cannot connect to client ReceivingInboundPort " + clientReceptionInboundURI, ex);
		}
	}

	@Override
	public boolean channelQuotaReached(String receptionPortURI) throws Exception {
		if (!registered.containsKey(receptionPortURI)) {
			throw new UnknownIdentifierException("unknown identifier " + receptionPortURI);
		}

		RegistrationClass rc = registered.get(receptionPortURI);
		int quota;

		switch (rc) {
			case STANDARD:
				quota = STANDARD_QUOTA;
				break;
			case PREMIUM:
				quota = PREMIUM_QUOTA;
				break;
			case FREE:
			default:
				quota = 0;
		}

		int current = createdChannelsByClient
				.getOrDefault(receptionPortURI, ConcurrentHashMap.newKeySet())
				.size();

		return current >= quota;
	}

	// 将 autorisedUsers 保存进哈希表
	@Override
	public void createChannel(String receptionPortURI, String channel, String autorisedUsers) throws Exception {
		if (!registered.containsKey(receptionPortURI)) {
			throw new UnknownIdentifierException("unknown identifier " + receptionPortURI);
		}

		if (channels.contains(channel)) {
			throw new AlreadyExistingChannelException("channel already exists " + channel);
		}

		RegistrationClass rc = registered.get(receptionPortURI);
		if (rc == RegistrationClass.FREE) {
			throw new ChannelQuotaExceededException("FREE client cannot create channels");
		}

		if (channelQuotaReached(receptionPortURI)) {
			throw new ChannelQuotaExceededException("quota exceeded for " + receptionPortURI);
		}

		channels.add(channel);
		channelCreators.put(channel, receptionPortURI);
		// 记录授权用户正则，如果为空则默认匹配所有 ".*"
		channelAuthorisations.put(channel, autorisedUsers == null ? ".*" : autorisedUsers);
		createdChannelsByClient
				.computeIfAbsent(receptionPortURI, k -> ConcurrentHashMap.newKeySet())
				.add(channel);
	}

	@Override
	public void destroyChannel(String receptionPortURI, String channel) throws Exception {
		if (!registered.containsKey(receptionPortURI)) {
			throw new UnknownIdentifierException("unknown identifier " + receptionPortURI);
		}

		if (!channels.contains(channel)) {
			throw new UnknownChannelException("unknown channel " + channel);
		}

		String creator = channelCreators.get(channel);
		if (creator == null || !creator.equals(receptionPortURI)) {
			throw new UnknownIdentifierException("client is not the creator of " + channel);
		}

		channels.remove(channel);
		channelCreators.remove(channel);
		channelAuthorisations.remove(channel); // 清理授权
		subscriptions.remove(channel);

		Set<String> created = createdChannelsByClient.get(receptionPortURI);
		if (created != null) {
			created.remove(channel);
		}
	}

	@Override
	public void destroyChannelNow(String receptionPortURI, String channel) throws Exception {
		destroyChannel(receptionPortURI, channel);
	}
}