package fr.sorbonne_u.publication.gossip;

/**
 * Types de messages gossip utilises pour la propagation d'evenements
 * entre les courtiers repartis.
 *
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public enum GossipPayloadType {

	/** Publication d'un ou plusieurs messages sur un canal. */
	PUBLISH,

	/** Enregistrement d'un nouveau client aupres du systeme. */
	REGISTER,

	/** Desenregistrement d'un client. */
	UNREGISTER,

	/** Creation d'un canal par un client privilegie. */
	CREATE_CHANNEL,

	/** Destruction d'un canal. */
	DESTROY_CHANNEL,

	/** Modification des autorisations d'acces a un canal. */
	MODIFY_AUTH
}
