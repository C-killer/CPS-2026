package fr.sorbonne_u.components.rt;

// Copyright Jacques Malenfant, Sorbonne Universite.
// Jacques.Malenfant@lip6.fr
//
// This software is a computer program whose purpose is to provide a
// basic component programming model to program with components
// distributed applications in the Java programming language.
//
// This software is governed by the CeCILL-C license under French law and
// abiding by the rules of distribution of free software.  You can use,
// modify and/ or redistribute the software under the terms of the
// CeCILL-C license as circulated by CEA, CNRS and INRIA at the following
// URL "http://www.cecill.info".
//
// As a counterpart to the access to the source code and  rights to copy,
// modify and redistribute granted by the license, users are provided only
// with a limited warranty  and the software's author,  the holder of the
// economic rights,  and the successive licensors  have only  limited
// liability. 
//
// In this respect, the user's attention is drawn to the risks associated
// with loading,  using,  modifying and/or developing or reproducing the
// software by the user in light of its specific status of free software,
// that may mean  that it is complicated to manipulate,  and  that  also
// therefore means  that it is reserved for developers  and  experienced
// professionals having in-depth computer knowledge. Users are therefore
// encouraged to load and test the software's suitability as regards their
// requirements in conditions enabling the security of their systems and/or 
// data to be ensured and,  more generally, to use and operate it in the 
// same conditions as regards security. 
//
// The fact that you are presently reading this means that you have had
// knowledge of the CeCILL-C license and that you accept its terms.

import java.io.Serializable;
import fr.sorbonne_u.components.exceptions.BCMException;
import fr.sorbonne_u.components.plugins.asynccall.AbstractAsyncCall;
import fr.sorbonne_u.components.plugins.asynccall.AsyncCallClientSidePlugin;
import fr.sorbonne_u.components.plugins.asynccall.AsyncCallResultReceptionCI;
import fr.sorbonne_u.components.plugins.asynccall.RemoteCompletableFuture;
import fr.sorbonne_u.components.plugins.asynccall.connections.AsyncCallResultReceptionInboundPort;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.components.rt.scheduling.tasks.AperiodicTaskActivation;
import fr.sorbonne_u.components.rt.scheduling.tasks.SporadicTask;
import fr.sorbonne_u.exceptions.AssertionChecking;
import fr.sorbonne_u.exceptions.PostconditionException;
import fr.sorbonne_u.exceptions.PreconditionException;

// -----------------------------------------------------------------------------
/**
 * The class <code>AsyncCallRTClientSidePlugin</code> implements a client side
 * asynchronous call plug-in for BCM4Java real time components.
 *
 * <p><strong>Description</strong></p>
 * 
 * <p>
 * Based on the implementation of {@code AsyncCallClientSidePlugin}, this
 * plug-in takes into account the fact that real time components execute their
 * code as real time tasks scheduled by a real time scheduler, hence the
 * inter-component calls must not delay the execution of tasks. The
 * {@code AsyncCallClientSidePlugin} ensures that calls are made asynchronously,
 * so the caller thread will not be blocked on the reception of the result of
 * the call but rather receives it by an asynchronous call back on the method
 * {@code receive}. But in {@code AsyncCallServerSidePlugin}, the call are
 * made asynchronous by using a server-side thread pool to which called code
 * is submitted through a {@code runTask}. This solution is not feasible for
 * real time components as it would interfere with the real time scheduling.
 * </p>
 * <p>
 * In the server side plug-in, the calls are made asynchronous first by
 * activating a sporadic task added to the scheduler task activations queues.
 * Hence, the methods {@code AsyncCallClientSidePlugin::asyncCall} and
 * {@code AsyncCallClientSidePlugin::asyncCallWithFuture} can be used without
 * change as they will return as soon as the sporadic task activation has been
 * added to the server side scheduler. As the server side real time component
 * threads are all under the control of its real time scheduler, the code
 * from the client side to the addition of the sporadic task activation to the
 * server side real time scheduler is execute by the client side thread.
 * </p>
 * <p>
 * When there is a result to be returned to the client side, the server side
 * uses the method {@code sendResult}, as in {@code AsyncCallServerSidePlugin},
 * and this calls in turn the method {@code receive} on this client side
 * plug-in. In {@code AsyncCallClientSidePlugin::receive}, the initial future
 * is retrieved from a concurrent hash map and completed with the received
 * result. As the removal of the future from the concurrent hash map may be
 * delayed by synchronisation, the result reception is also executed as a
 * sporadic real time task activation created by
 * {@code AsyncCallRTClientSidePlugin::receive} and added to the client side
 * real time scheduler. The client real time component inherits the method
 * {@code AbstractRTComponent::receiveResult} meant to be the sporadic task
 * to be activated. However, when declaring real time tasks, an annotation
 * must be put on the method that enumerates the constraints of this task,
 * like its WCET and its relative deadline, information that can only be known
 * at the user level component. Hence, the idea is that
 * {@code AbstractRTComponent::receiveResult} must be redefined in the user real
 * time component class to put the required {@code SporadicTaskDescription}
 * annotation, and in its body it simply calls {@code super.receiveResult}
 * with the same actual parameters.
 * </p>
 * <p>
 * From the client side code point of view, calling another real time component
 * is totally the same as for the basic asynchronous call plug-in. A call to
 * the plug-in {@code AsyncCallClientSidePlugin::asyncCall} and
 * {@code AsyncCallClientSidePlugin::asyncCallWithFuture} methods is made.
 * When using the version with future, a {@code RemoteCompletableFuture} is
 * returned and must be used by the client code to get its result when it
 * will be available. However, a client real time component code should never
 * block on a synchronisation. The right pattern here is for a real time task
 * to check for the availability of the result with the method {@code isDone}
 * and to get the actual result only when done otherwise postpone the use of the
 * result to a later activation of the task requiring it.
 * </p>
 * 
 * <p><strong>Implementation Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * <p><strong>Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * <p>Created on : 2025-07-16</p>
 * 
 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
 */
public class			AsyncCallRTClientSidePlugin
extends		AsyncCallClientSidePlugin
{
	// -------------------------------------------------------------------------
	// Constants and variables
	// -------------------------------------------------------------------------

	private static final long serialVersionUID = 1L;

	/** when true, the component adds debugging traces to its outputs.		*/
	public static boolean			DEBUG = false;

	// -------------------------------------------------------------------------
	// Invariants
	// -------------------------------------------------------------------------

	/**
	 * return true if the implementation invariants are observed, false otherwise.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code instance != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param instance	instance to be tested.
	 * @return			true if the implementation invariants are observed, false otherwise.
	 */
	protected static boolean	implementationInvariants(
		AsyncCallRTClientSidePlugin instance
		)
	{
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		ret &= AssertionChecking.checkImplementationInvariant(
					true,
					AsyncCallRTClientSidePlugin.class,
					instance,
					"");
		return ret;
	}

	/**
	 * return true if the invariants are observed, false otherwise.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code instance != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param instance	instance to be tested.
	 * @return			true if the invariants are observed, false otherwise.
	 */
	protected static boolean	invariants(AsyncCallRTClientSidePlugin instance)
	{
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		ret &= AssertionChecking.checkInvariant(
					true,
					AsyncCallRTClientSidePlugin.class,
					instance,
					"");
		return ret;
	}

	// -------------------------------------------------------------------------
	// Constructors
	// -------------------------------------------------------------------------

	/**
	 * create an new plug-in instance.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 */
	public			AsyncCallRTClientSidePlugin()
	{
		super();
	}

	// -------------------------------------------------------------------------
	// Methods
	// -------------------------------------------------------------------------

	/**
	 * @see fr.sorbonne_u.components.plugins.asynccall.AsyncCallClientSidePlugin#asyncCall(fr.sorbonne_u.components.plugins.asynccall.AbstractAsyncCall)
	 */
	@Override
	public void			asyncCall(AbstractAsyncCall c) throws Exception
	{
		if (DEBUG) {
			this.getOwner().logMessage(
					"New call\n--------\n" +
					"AsyncCallRTClientSidePlugin::asyncCall "
					+ c.getClass().getSimpleName() + " "
					+ Thread.currentThread());
		}

		super.asyncCall(c);
	}

	/**
	 * @see fr.sorbonne_u.components.plugins.asynccall.AsyncCallClientSidePlugin#asyncCallWithFuture(fr.sorbonne_u.components.plugins.asynccall.AbstractAsyncCall)
	 */
	@Override
	public RemoteCompletableFuture<Serializable>	asyncCallWithFuture(
		AbstractAsyncCall c
		) throws Exception
	{
		if (DEBUG) {
			this.getOwner().logMessage(
					"New call\n--------\n" +
					"AsyncCallRTClientSidePlugin::asyncCallWithFuture "
					+ c.getClass().getSimpleName() + " "
					+ Thread.currentThread());
		}

		return super.asyncCallWithFuture(c);
	}

	/**
	 * receive a result from an asynchronous call to another component but using
	 * a local sporadic task activation to do so.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code callURI != null && !callURI.isEmpty()}
	 * post	{@code true}	// no more postconditions.
	 * </pre>
	 * 
	 * @see fr.sorbonne_u.components.plugins.asynccall.AsyncCallClientSidePlugin#receive(java.lang.String, java.io.Serializable)
	 */
	@Override
	public void			receive(String callURI, Serializable result)
	{
		assert	callURI != null && !callURI.isEmpty() :
				new PreconditionException(
						"callURI != null && !callURI.isEmpty()");

		if (DEBUG) {
			this.getOwner().logMessage(
					"AsyncCallRTClientSidePlugin::receive " 
					+ Thread.currentThread());
		}

		SporadicTask task =
			(SporadicTask)
				((AbstractRTComponent)this.getOwner()).
						getScheduler().getRealTimeTask(
								AbstractRTComponent.RESULT_RECEPTION_TASK_KEY);
		AperiodicTaskActivation ata =
							task.activateNow(new Object[]{callURI, result});
		((AbstractRTComponent)this.getOwner()).
							getScheduler().addTaskActivationAndDispatch(ata);
	}

	/**
	 * forward the call to the method {@code super.receive}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code callURI != null && !callURI.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param callURI		URI attributed to the call when it was passed to the server.
	 * @param result		the result of the call coming back from the server.
	 * @throws BCMException	<i>to do</i>.
	 */
	public void			performSuperReceived(
		String callURI,
		Serializable result
		) throws BCMException
	{
		if (DEBUG) {
			this.getOwner().logMessage(
					"AsyncCallRTClientSidePlugin::performSuperReceived "
					+ Thread.currentThread() + "\n--------\nendcall\n");
		}

		super.receive(callURI, result);
	}

	// -------------------------------------------------------------------------
	// Internal methods
	// -------------------------------------------------------------------------

	/**
	 * @see fr.sorbonne_u.components.plugins.asynccall.AsyncCallClientSidePlugin#createResultReceptionInboundPort()
	 */
	@Override
	protected AbstractInboundPort	createResultReceptionInboundPort()
	throws Exception
	{
		AbstractInboundPort ret =
				new AsyncCallResultReceptionInboundPort(
							this.getOwner(),
							this.getPluginURI());

		assert	AsyncCallResultReceptionCI.class.isAssignableFrom(ret.getClass()) :
				new PostconditionException(
						"AsyncCallResultReceptionCI.class.isAssignableFrom("
						+ "ret.getClass())");
		assert	this.resultReceptionInterface.isAssignableFrom(ret.getClass()) :
				new BCMException(
						"resultReceptionInterface.isAssignableFrom("
						+ "ret.getClass())"); 

		return ret;
	}
}
// -----------------------------------------------------------------------------
