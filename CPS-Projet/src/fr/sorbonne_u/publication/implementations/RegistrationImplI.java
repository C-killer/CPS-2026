package fr.sorbonne_u.publication.implementations;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;

/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public interface RegistrationImplI {

	boolean registered(String receptionPortURI) throws Exception;

	boolean registered(String receptionPortURI, RegistrationClass rc) throws Exception;

	String register(String receptionPortURI, RegistrationClass rc) throws Exception;

	String modifyServiceClass(String receptionPortURI, RegistrationClass rc)
			throws Exception;

	void unregister(String receptionPortURI) throws Exception;

	boolean channelExist(String channel) throws Exception;

	boolean channelAuthorised(String receptionPortURI, String channel) throws Exception;

	boolean subscribed(String receptionPortURI, String channel) throws Exception;

	void subscribe(String receptionPortURI, String channel, MessageFilterI filter)
			throws Exception;

	void unsubscribe(String receptionPortURI, String channel)
			throws Exception;

	boolean modifyFilter(String receptionPortURI, String channel, MessageFilterI filter)
			throws Exception;
}
