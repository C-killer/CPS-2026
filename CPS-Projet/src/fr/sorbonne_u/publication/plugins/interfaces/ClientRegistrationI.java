package fr.sorbonne_u.publication.plugins.interfaces;

import fr.sorbonne_u.components.PluginI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;

/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public interface ClientRegistrationI extends PluginI {
	boolean registered();

	boolean registered(RegistrationClass rc) throws Exception;

	void register(RegistrationClass rc) throws Exception;

	void modifyServiceClass(RegistrationClass rc) throws Exception;

	void unregister() throws Exception;
}