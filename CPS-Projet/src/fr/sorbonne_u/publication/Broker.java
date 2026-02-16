package fr.sorbonne_u.publication;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.cps.pubsub.exceptions.AlreadyRegisteredException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownIdentifierException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.publication.connectors.ReceivingConnector;
import fr.sorbonne_u.publication.implementations.PublishingImplI;
import fr.sorbonne_u.publication.implementations.RegistrationImplI;
import fr.sorbonne_u.publication.ports.PublishingInbound;
import fr.sorbonne_u.publication.ports.ReceivingOutbound;
import fr.sorbonne_u.publication.ports.RegistrationInbound;

public class Broker extends AbstractComponent
		implements RegistrationImplI, PublishingImplI {
	// Inbound ports offered by the broker
	protected final RegistrationInbound registrationInboundPort;
	protected final PublishingInbound publishingInboundPort;

	// URIs
	protected static final String REGISTRATION_INBOUND_URI = "broker-registration";
	protected static final String PUBLISHING_INBOUND_URI = "broker-publishing";

	// FREE: static channels channel0..channel{N-1}
	protected final Set<String> channels = new HashSet<>();

	// Registered clients: receptionPortURI -> RegistrationClass
	protected final Map<String, RegistrationClass> registered = new HashMap<>();

	// Subscriptions: channel -> (receptionPortURI -> filter)
	protected final Map<String, Map<String, MessageFilterI>> subscriptions = new HashMap<>();

	// Broker -> client connection cache: receptionPortURI -> outbound port
	// connected to client's ReceivingInbound
	protected final Map<String, ReceivingOutbound> receivingOutboundPorts = new HashMap<>();

	public Broker(int nbChannels) throws Exception {
		super(1, 0);

		for (int i = 0; i < nbChannels; i++) {
			channels.add("channel" + i);
		}

		this.registrationInboundPort = new RegistrationInbound(REGISTRATION_INBOUND_URI, this);
		this.registrationInboundPort.publishPort();

		this.publishingInboundPort = new PublishingInbound(PUBLISHING_INBOUND_URI, this);
		this.publishingInboundPort.publishPort();
	}

	// Required by cahier des charges for discovery (Audit 1 needs it in CVM)
	public static String registrationPortURI() {
		return REGISTRATION_INBOUND_URI;
	}

	// ---------------------------------------------------------------------
	// RegistrationImplI (signatures MUST match your interface exactly)
	// ---------------------------------------------------------------------

	@Override
	public boolean registered(String receptionPortURI) throws RemoteException {
		return registered.containsKey(receptionPortURI);
	}

	@Override // 注册+registration等级
	public boolean registered(String receptionPortURI, RegistrationClass rc) throws RemoteException {
		return rc != null && rc.equals(registered.get(receptionPortURI));
	}

	@Override
	public String register(String receptionPortURI, RegistrationClass rc)
			throws RemoteException, AlreadyRegisteredException {
		if (receptionPortURI == null || rc == null) {
			throw new RemoteException("register: null parameter.");
		}
		if (registered.containsKey(receptionPortURI)) {
			throw new AlreadyRegisteredException("register: already registered " + receptionPortURI);
		}
		registered.put(receptionPortURI, rc);

		return PUBLISHING_INBOUND_URI;
	}

	@Override // 修改服务等级
	public String modifyServiceClass(String receptionPortURI, RegistrationClass rc)
			throws RemoteException, AlreadyRegisteredException {
		if (!registered.containsKey(receptionPortURI)) {
			throw new AlreadyRegisteredException("modifyServiceClass: unknown identifier " + receptionPortURI);
		}
		registered.put(receptionPortURI, rc);
		// For now, Audit 1 still uses the same Publishing inbound.
		return PUBLISHING_INBOUND_URI;
	}

	@Override // 客户端离开,清除连接,删除记录
	public void unregister(String receptionPortURI) throws RemoteException, UnknownIdentifierException {
		if (!registered.containsKey(receptionPortURI)) {
			throw new UnknownIdentifierException("unregister: unknown identifier " + receptionPortURI);
		}
		registered.remove(receptionPortURI);

		// Remove subscriptions
		for (Map<String, MessageFilterI> subs : subscriptions.values()) {
			subs.remove(receptionPortURI);
		}

		// Disconnect cached outbound port if any
		ReceivingOutbound rop = receivingOutboundPorts.remove(receptionPortURI);
		if (rop != null) {
			try {
				rop.doDisconnection();
				rop.unpublishPort();
			} catch (Exception e) {
				throw new RemoteException("unregister: error during disconnection.", e);
			}
		}
	}

	@Override
	public boolean channelExists(String channel) throws RemoteException {
		return channels.contains(channel);
	}

	@Override
	public boolean subscribed(String receptionPortURI, String channel) throws RemoteException {
		Map<String, MessageFilterI> subs = subscriptions.get(channel);
		return subs != null && subs.containsKey(receptionPortURI);
	}

	@Override // 订阅
	public void subscribe(String receptionPortURI, String channel, MessageFilterI filter) throws RemoteException {
		if (!channels.contains(channel)) {
			throw new RemoteException("subscribe: unknown channel " + channel);
		}
		if (!registered.containsKey(receptionPortURI)) {
			throw new RemoteException("subscribe: not registered " + receptionPortURI);
		}
		subscriptions.computeIfAbsent(channel, k -> new HashMap<>())
				.put(receptionPortURI, filter);
	}

	@Override // 取消订阅
	public void unsubscribe(String receptionPortURI, String channel) throws RemoteException {
		if (!channels.contains(channel)) {
			throw new RemoteException("unsubscribe: unknown channel " + channel);
		}
		Map<String, MessageFilterI> subs = subscriptions.get(channel);
		if (subs != null) {
			subs.remove(receptionPortURI);
		}
	}

	@Override // 更新过滤器
	public boolean modifyFilter(String receptionPortURI, String channel, MessageFilterI filter) throws RemoteException {
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

	// ---------------------------------------------------------------------
	// PublishingImplI (signatures MUST match your interface exactly)
	// ---------------------------------------------------------------------

	@Override
	public void publish(String receptionPortURI, String channel, MessageI message) throws RemoteException {
		if (!channels.contains(channel)) {
			throw new RemoteException("publish: unknown channel " + channel);
		}
		// Audit 1: allow publishing even if not registered? Spec implies clients
		// register first;
		// here we enforce "registered" to avoid weird states.
		if (!registered.containsKey(receptionPortURI)) {
			throw new RemoteException("publish: not registered " + receptionPortURI);
		}

		deliverToSubscribers(channel, new MessageI[] { message });
	}

	@Override
	public void publish(String receptionPortURI, String channel, ArrayList<MessageI> messages) throws RemoteException {
		if (!channels.contains(channel)) {
			throw new RemoteException("publish: unknown channel " + channel);
		}
		if (!registered.containsKey(receptionPortURI)) {
			throw new RemoteException("publish: not registered " + receptionPortURI);
		}
		if (messages == null || messages.isEmpty())
			return;

		deliverToSubscribers(channel, messages.toArray(new MessageI[0]));
	}

	// ---------------------------------------------------------------------
	// Internal delivery helper
	// ---------------------------------------------------------------------

	// 1.找到该频道的所有订阅者
	// 2.对每条消,调用订阅者的过滤器进行匹配
	// 3.如果匹配成功,则通过ReceivingOutbound端口将消
	protected void deliverToSubscribers(String channel, MessageI[] messages) throws RemoteException {
		Map<String, MessageFilterI> subs = subscriptions.get(channel);
		if (subs == null || subs.isEmpty())
			return;

		for (Map.Entry<String, MessageFilterI> e : subs.entrySet()) {
			String clientReceptionInboundURI = e.getKey();
			MessageFilterI filter = e.getValue();

			// Filter on each message; deliver individually for now (simple Audit 1)
			for (MessageI m : messages) {
				if (filter == null || filter.match(m)) {
					ReceivingOutbound rop = getOrConnectReceivingOutbound(clientReceptionInboundURI);
					rop.receive(channel, m); // NOTE: ReceivingCI requires channel param
												// :contentReference[oaicite:4]{index=4}
				}
			}
		}
	}

	// 如果 Broker 还没连接过某个客户端，它会在此刻动态创建一个出口端口并完成连接,如果已经连接过，则直接复用已有的端口，提高效率。
	protected ReceivingOutbound getOrConnectReceivingOutbound(String clientReceptionInboundURI) throws RemoteException {
		ReceivingOutbound rop = receivingOutboundPorts.get(clientReceptionInboundURI);
		if (rop != null)
			return rop;

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
}