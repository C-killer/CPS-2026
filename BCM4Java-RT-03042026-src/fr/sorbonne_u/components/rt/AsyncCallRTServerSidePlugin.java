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

import fr.sorbonne_u.components.plugins.asynccall.AsyncCallServerSidePlugin;
import fr.sorbonne_u.components.plugins.asynccall.connections.AsyncCallInboundPort;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.components.rt.asynccall.AbstractRTAsyncCall;
import fr.sorbonne_u.exceptions.AssertionChecking;
import fr.sorbonne_u.exceptions.PostconditionException;
import fr.sorbonne_u.exceptions.PreconditionException;
import java.io.Serializable;
import fr.sorbonne_u.components.exceptions.BCMException;
import fr.sorbonne_u.components.plugins.asynccall.AbstractAsyncCall;
import fr.sorbonne_u.components.plugins.asynccall.AsyncCallCI;

// -----------------------------------------------------------------------------
/**
 * The class <code>AsyncCallRTServerSidePlugin</code> implements a server side
 * asynchronous call plug-in for BCM4Java real time components.
 *
 * <p><strong>Description</strong></p>
 * 
 * <p>
 * Based on the implementation of {@code AsyncCallServerSidePlugin}, this
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
 * In this server side plug-in, the calls are made asynchronous first by taking
 * into account the fact that code is executed as real time task activations.
 * Hence, when a call is made by a component to a real time component, rather
 * than being executed directly as a task submitted to a thread pool by
 * {@code runTask}, the method {@code asyncCall} calls
 * {@code AbstractRTAsyncCall::execute} which in turn creates and activates
 * immediately a sporadic real time task created for the real time component
 * method and put it in the scheduler tasks queues to be executed according to
 * the scheduler policy. As the server real time component threads are all under
 * the control of the scheduler, the method {@code asyncCall} is executed by the
 * caller thread, but it will return as soon as the real time task activation is
 * added to the scheduler task activations queues, hence limiting its execution
 * time to the benefit of real time component callers.
 * </p>
 * <p>
 * When there is a result to be returned to the client side, the server side
 * uses the method {@code sendResult}, as in {@code AsyncCallServerSidePlugin}.
 * The call to this method is made as part of the real time task activated to
 * answer the call, hence by the scheduled thread.
 * </p>
 * 
 * <p><strong>Implementation Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code scheduler != null}
 * </pre>
 * 
 * <p><strong>Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * <p>Created on : 2025-07-10</p>
 * 
 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
 */
public class			AsyncCallRTServerSidePlugin
extends		AsyncCallServerSidePlugin
{
	// -------------------------------------------------------------------------
	// Constants and variables
	// -------------------------------------------------------------------------

	/** when true, the component adds debugging traces to its outputs.		*/
	public static boolean		DEBUG = false;

	private static final long	serialVersionUID = 1L;
	/** scheduler of the owner real time component used to execute the
	 *  sporadic task which code is the method called from a client
	 *  component.															*/
	protected GenericScheduler	scheduler;

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
		AsyncCallRTServerSidePlugin instance
		)
	{
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		ret &= AssertionChecking.checkImplementationInvariant(
				instance.scheduler != null,
				AsyncCallRTServerSidePlugin.class,
				instance,
				"scheduler != null");
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
	protected static boolean	invariants(
		AsyncCallRTServerSidePlugin instance
		)
	{
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		return ret;
	}

	// -------------------------------------------------------------------------
	// Constructors
	// -------------------------------------------------------------------------

	/**
	 * create a new asynchronous call real time server side plug-in instance
	 * which will use the standard request handler executor service to execute
	 * its code.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 */
	public				AsyncCallRTServerSidePlugin()
	{
		super();
	}

	// -------------------------------------------------------------------------
	// Methods
	// -------------------------------------------------------------------------

	/**
	 * set the scheduler associated with the real time component owning this
	 * plug-in.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code s.getOwner() == this.getOwner()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param s	scheduler associated with the real time component owning this plug-in.
	 */
	public void			setScheduler(GenericScheduler s)
	{
		assert	s.getOwner() == this.getOwner() :
				new PreconditionException("s.getOwner() == this.getOwner()");

		this.scheduler = s;
	}

	/**
	 * call the server side component to execute a method through the command
	 * {@code c}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code c != null}
	 * pre	{@code c instanceof AbstractRTAsyncCall}
	 * post	{@code true}	// no more postconditions.
	 * </pre>
	 * 
	 * @see fr.sorbonne_u.components.plugins.asynccall.AsyncCallServerSidePlugin#asyncCall(fr.sorbonne_u.components.plugins.asynccall.AbstractAsyncCall)
	 */
	@Override
	public void			asyncCall(AbstractAsyncCall c) throws Exception
	{
		assert	c != null : new PreconditionException("c != null");
		assert	c instanceof AbstractRTAsyncCall :
				new PreconditionException("c instanceof AbstractRTAsyncCall");

		((AbstractRTAsyncCall)c).setCalleeInfo(
				(AbstractRTComponent)this.getOwner(), this, this.scheduler);
		c.execute();
	}

	/**
	 * @see fr.sorbonne_u.components.plugins.asynccall.AsyncCallServerSidePlugin#sendResult(java.lang.String, java.io.Serializable, java.lang.String)
	 */
	@Override
	public void			sendResult(
		String callURI,
		Serializable result,
		String receptionPortURI
		) throws Exception
	{
		if (DEBUG) {
			this.getOwner().traceMessage(
					"AsyncCallRTServerSidePlugin::sendResult("
					+ callURI + ", " + result + ", " + receptionPortURI + ") "
					+ Thread.currentThread()) ;
		}
		 
		super.sendResult(callURI, result, receptionPortURI);
	}

	// -------------------------------------------------------------------------
	// Internal methods
	// -------------------------------------------------------------------------

	/**
	 * @see fr.sorbonne_u.components.plugins.asynccall.AsyncCallServerSidePlugin#createAsyncCallInboundPort()
	 */
	@Override
	protected AbstractInboundPort	createAsyncCallInboundPort()
	throws Exception
	{
		AbstractInboundPort ret =
				new AsyncCallInboundPort(
						this.getOwner(),
						this.getPluginURI());

		assert	AsyncCallCI.class.isAssignableFrom(ret.getClass()) :
				new PostconditionException(
						"AsyncCallCI.class.isAssignableFrom(ret.getClass()");
		assert	this.asyncCallOfferedInterface.isAssignableFrom(ret.getClass()) :
				new BCMException(
						"asyncCallOfferedInterface.isAssignableFrom("
						+ "ret.getClass())");

		return ret;
	}
}
// -----------------------------------------------------------------------------
