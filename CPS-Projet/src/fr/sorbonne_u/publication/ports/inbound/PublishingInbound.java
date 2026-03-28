package fr.sorbonne_u.publication.ports.inbound;

import java.rmi.RemoteException;
import java.util.ArrayList;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.publication.implementations.PublishingImplI;

/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class PublishingInbound extends AbstractInboundPort implements PublishingCI {

	private static final long serialVersionUID = 1L;

	public PublishingInbound(ComponentI owner) throws Exception {
		super(PublishingCI.class, owner);
		assert owner instanceof PublishingImplI : "Owner must implement PublishingImplI to use PublishingInbound.";
	}

	public PublishingInbound(String uri, ComponentI owner) throws Exception {
		super(uri, PublishingCI.class, owner);
		assert owner instanceof PublishingImplI : "Owner must implement PublishingImplI to use PublishingInbound.";
	}

	@Override
	public void publish(String receptionPortURI, String channel, MessageI message)
			throws RemoteException, UnknownChannelException {
		try {
			this.getOwner().runTask(o -> {
				try {
					((PublishingImplI) o).publish(receptionPortURI, channel, message);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			});
		} catch (Exception e) {
			throw new RemoteException(
					"Cannot schedule async publish(receptionPortURI, channel, message)", e);
		}
	}

	@Override
	public void publish(String receptionPortURI, String channel, ArrayList<MessageI> messages)
			throws RemoteException, UnknownChannelException {
		try {
			this.getOwner().runTask(o -> {
				try {
					((PublishingImplI) o).publish(receptionPortURI, channel, messages);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			});
		} catch (Exception e) {
			throw new RemoteException(
					"Cannot schedule async publish(receptionPortURI, channel, messages)", e);
		}
	}

	public void asyncPublishAndNotify(String receptionPortURI, String channel, MessageI message,
			String notificationInboundPortURI) throws Exception {
		try {
			this.getOwner().runTask(o -> {
				try {
					((PublishingImplI) o).asyncPublishAndNotify(receptionPortURI, channel, message,
							notificationInboundPortURI);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			});
		} catch (Exception e) {
			throw new RemoteException(
					"Cannot schedule asyncPublishAndNotify(receptionPortURI, channel, message, notificationInboundPortURI)",
					e);
		}
	}

	public void asyncPublishAndNotify(String receptionPortURI, String channel, ArrayList<MessageI> messages,
			String notificationInboundPortURI) throws Exception {
		try {
			this.getOwner().runTask(o -> {
				try {
					((PublishingImplI) o).asyncPublishAndNotify(receptionPortURI, channel, messages,
							notificationInboundPortURI);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			});
		} catch (Exception e) {
			throw new RemoteException(
					"Cannot schedule asyncPublishAndNotify(receptionPortURI, channel, messages, notificationInboundPortURI)",
					e);
		}
	}
}