package fr.sorbonne_u.publication.plugins.interfaces;

import java.util.ArrayList;

import fr.sorbonne_u.components.PluginI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;

/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public interface ClientPublicationI extends PluginI {
	boolean channelExist(String channel);

	boolean channelAuthorised(String channel) throws Exception;

	void publish(String channel, MessageI message) throws Exception;

	void publish(String channel, ArrayList<MessageI> messages) throws Exception;

	void asyncPublishAndNotify(String channel, MessageI message) throws Exception;

	void asyncPublishAndNotify(String channel, ArrayList<MessageI> messages) throws Exception;
}