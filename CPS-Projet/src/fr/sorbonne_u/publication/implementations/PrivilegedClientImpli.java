package fr.sorbonne_u.publication.implementations;

import java.rmi.RemoteException;

import fr.sorbonne_u.cps.pubsub.exceptions.AlreadyExistingChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownIdentifierException;
import fr.sorbonne_u.cps.pubsub.interfaces.ChannelQuotaExceededException;

//semaine4
public interface PrivilegedClientImpli {

	boolean channelExists(String channel) throws RemoteException;

	/*
	 * channelQuotaReached verifie si un composant a atteint son quota de canaux
	 * 检查组件是否已达到其通道配额
	 */
	boolean channelQuotaReached(String receptionPortURI) throws RemoteException,
			UnknownIdentifierException;

	/*
	 * createChannel permet ainsi de créer un canal en spécifiant son nom et une
	 * expressions régulière qui devra apparier les URIs
	 * 允许创建一个通道,需指定其名称以及一个用于匹配允许使用该通道组件URI
	 */
	void createChannel(String receptionPortURI, String channel) throws RemoteException,
			UnknownIdentifierException,
			AlreadyExistingChannelException,
			ChannelQuotaExceededException;

	/*
	 * destroyChannel arrête immédiatement la publication des messages sur lecanal
	 * (les publieurs recevront l'exception UnknownChannelException)
	 * puis termine l'envoides messages encore dans la file dattente avant de
	 * détruire le canal.
	 * 即停止在该通道上发布消(发布者将收到UnknownChannelException),并在销毁通道前完成发送队列中剩余的消息
	 */
	void destroyChannel(String receptionPortURI, String channel) throws RemoteException,
			UnknownIdentifierException,
			UnknownChannelException;

	/*
	 * destroyChannelNow arrête immédiatement la publication et détruit le canal y
	 * compris tous les messages en file d'attente non encore expédiés
	 * 立即停止发布并销毁通道，包括所有尚未发送的队列消息
	 */
	void destroyChannelNow(String receptionPortURI, String channel) throws RemoteException,
			UnknownIdentifierException,
			UnknownChannelException;
}
