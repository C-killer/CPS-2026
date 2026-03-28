package fr.sorbonne_u.publication.implementations;

//semaine4
/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public interface PrivilegedClientImpli extends PublishingImplI {

	boolean hasCreatedChannel(String receptionPortURI, String channel) throws Exception;

	boolean channelExist(String channel) throws Exception;

	boolean isAuthorisedUser(String channel, String uri) throws Exception;

	void modifyAuthorisedUsers(String receptionPortURI, String channel, String autorisedUsers) throws Exception;

	void removeAuthorisedUsers(String receptionPortURI, String channel, String regularExpression) throws Exception;

	/*
	 * channelQuotaReached verifie si un composant a atteint son quota de canaux
	 * 检查组件是否已达到其通道配额
	 */
	boolean channelQuotaReached(String receptionPortURI) throws Exception;

	/*
	 * createChannel permet ainsi de créer un canal en spécifiant son nom et une
	 * expressions régulière qui devra apparier les URIs
	 * 允许创建一个通道,需指定其名称以及一个用于匹配允许使用该通道组件URI
	 */
	void createChannel(String receptionPortURI, String channel, String autorisedUsers) throws Exception;

	/*
	 * destroyChannel arrête immédiatement la publication des messages sur lecanal
	 * (les publieurs recevront l'exception UnknownChannelException)
	 * puis termine l'envoides messages encore dans la file dattente avant de
	 * détruire le canal.
	 * 即停止在该通道上发布消(发布者将收到UnknownChannelException),并在销毁通道前完成发送队列中剩余的消息
	 */
	void destroyChannel(String receptionPortURI, String channel) throws Exception;

	/*
	 * destroyChannelNow arrête immédiatement la publication et détruit le canal y
	 * compris tous les messages en file d'attente non encore expédiés
	 * 立即停止发布并销毁通道，包括所有尚未发送的队列消息
	 */
	void destroyChannelNow(String receptionPortURI, String channel) throws Exception;
}
