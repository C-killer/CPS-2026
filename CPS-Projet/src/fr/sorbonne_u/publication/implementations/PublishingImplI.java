package fr.sorbonne_u.publication.implementations;

import java.util.ArrayList;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;

/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public interface PublishingImplI {
	void publish(String receptionPortURI, String channel, MessageI message)
			throws Exception;

	void publish(String receptionPortURI, String channel, ArrayList<MessageI> messages)
			throws Exception;

	/*
	 * 异步执行
	 * Exécution asynchrone (Async)
	 */
	void asyncPublishAndNotify(String receptionPortURI, String channel, MessageI message,
			String notificationInbounhdPortURI) throws Exception;

	void asyncPublishAndNotify(String receptionPortURI, String channel, ArrayList<MessageI> messages,
			String notificationInbounhdPortURI) throws Exception;
}
