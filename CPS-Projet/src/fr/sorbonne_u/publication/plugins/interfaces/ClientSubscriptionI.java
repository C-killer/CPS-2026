package fr.sorbonne_u.publication.plugins.interfaces;

import java.time.Duration;
import java.util.concurrent.Future;

import fr.sorbonne_u.components.PluginI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;

/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public interface ClientSubscriptionI extends PluginI {
	boolean channelExist(String channel) throws Exception;

	boolean channelAuthorised(String channel) throws Exception;

	boolean subscribed(String channel) throws Exception;

	void subscribe(String channel, MessageFilterI filter) throws Exception;

	void unsubscribe(String channel) throws Exception;

	void modifyFilter(String channel, MessageFilterI filter) throws Exception;

	void receive(String channel, MessageI message);

	void receive(String channel, MessageI[] messages);

	MessageI waitForNextMessage(String channel) throws Exception;

	MessageI waitForNextMessage(String channel, Duration d) throws Exception;

	Future<MessageI> getNextMessage(String channel) throws Exception;
}