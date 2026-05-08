package fr.sorbonne_u.components.rt.asynccall;

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
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.exceptions.BCMRuntimeException;
import fr.sorbonne_u.components.plugins.asynccall.AbstractAsyncCall;
import fr.sorbonne_u.components.plugins.asynccall.AsyncCallServerSidePlugin;
import fr.sorbonne_u.components.rt.AbstractRTComponent;
import fr.sorbonne_u.components.rt.GenericScheduler;
import fr.sorbonne_u.components.rt.scheduling.SchedulingI.TaskKeyI;
import fr.sorbonne_u.components.rt.scheduling.tasks.AperiodicTaskActivation;
import fr.sorbonne_u.components.rt.scheduling.tasks.SporadicTask;
import fr.sorbonne_u.exceptions.PreconditionException;

// -----------------------------------------------------------------------------
/**
 * The class <code>AbstractRTAsyncCall</code> implements a form of command
 * pattern to provide a limited asynchronous call with future capability
 * to BCM4Java real time components.
 *
 * <p><strong>Description</strong></p>
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
 * <p>Created on : 2025-07-10</p>
 * 
 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
 */
public abstract class	AbstractRTAsyncCall
extends		AbstractAsyncCall
{
	// -------------------------------------------------------------------------
	// Constants and variables
	// -------------------------------------------------------------------------

	private static final long serialVersionUID = 1L;

	/** when true, add debugging traces to its outputs.						*/
	public static boolean		DEBUG = true;

	/** task key of the task corresponding to the method that is called.	*/
	protected final TaskKeyI	calledMethodTaskKey;
	/** scheduler responsible of the task activation.						*/
	protected GenericScheduler	scheduler;

	// -------------------------------------------------------------------------
	// Constructors
	// -------------------------------------------------------------------------

	/**
	 * create a real time asynchronous call for the method designated by
	 * {@code calledMehodTaskKey}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code calledMehodTaskKey != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param calledMehodTaskKey	task key of the task corresponding to the method that is called.
	 */
	public				AbstractRTAsyncCall(TaskKeyI calledMehodTaskKey)
	{
		super();

		assert	calledMehodTaskKey != null :
				new PreconditionException("calledMehodTaskKey");

		this.calledMethodTaskKey = calledMehodTaskKey;
	}

	/**
	 * create a real time asynchronous call for the method designated by
	 * {@code calledMehodTaskKey} with the given actual parameters.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code calledMehodTaskKey != null}
	 * pre	{@code params != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param calledMehodTaskKey	task key of the task corresponding to the method that is called.
	 * @param params				actual parameters of the call.
	 */
	public				AbstractRTAsyncCall(
		TaskKeyI calledMehodTaskKey,
		Serializable[] params
		)
	{
		super(params);
		
		this.calledMethodTaskKey = calledMehodTaskKey;
	}

	// -------------------------------------------------------------------------
	// Methods
	// -------------------------------------------------------------------------

	/**
	 * @see fr.sorbonne_u.components.plugins.asynccall.AbstractAsyncCall#calleeInfoSet()
	 */
	@Override
	public boolean		calleeInfoSet()
	{
		return super.calleeInfoSet() && this.scheduler != null;
	}

	/**
	 * @see fr.sorbonne_u.components.plugins.asynccall.AbstractAsyncCall#setCalleeInfo(fr.sorbonne_u.components.AbstractComponent, fr.sorbonne_u.components.plugins.asynccall.AsyncCallServerSidePlugin)
	 */
	@Override
	public void			setCalleeInfo(
		AbstractComponent server,
		AsyncCallServerSidePlugin plugin
		) throws Exception
	{
		throw new BCMRuntimeException(
					"must call setCalleeInfo(AbstractComponent, "
					+ "AsyncCallServerSidePlugin, scheduler)");
	}

	/**
	 * set the information about the server-side that will execute this
	 * asynchronous real time call command.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code server != null}
	 * pre	{@code plugin != null}
	 * pre	{@code server.isInstalled(plugin.getPluginURI())}
	 * pre	{@code scheduler != null}
	 * pre	{@code server.isScheduler(scheduler)}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param server		component that executes the method called through this command.
	 * @param plugin		plug-in managing the asynchronous call within {@code server}.
	 * @param scheduler		scheduler of the real time component {@code server}.
	 * @throws Exception	<i>to do</i>.
	 */
	public void			setCalleeInfo(
		AbstractRTComponent server,
		AsyncCallServerSidePlugin plugin,
		GenericScheduler scheduler
		) throws Exception
	{
		// Preconditions checking
		assert	server != null : new PreconditionException("server != null");
		assert	plugin != null : new PreconditionException("plugin != null");
		assert	server.isInstalled(plugin.getPluginURI()) :
				new PreconditionException(
						"server.isInstalled(plugin.getPluginURI())");
		assert	scheduler != null :
				new PreconditionException("scheduler != null");
		assert	server.isScheduler(scheduler) :
				new PreconditionException("server.isScheduler(scheduler)");

		super.setCalleeInfo(server, plugin);
		this.scheduler = scheduler;
	}

	/**
	 * @see fr.sorbonne_u.components.plugins.asynccall.AsyncCallI#execute()
	 */
	@Override
	public void			execute() throws Exception
	{
		if (DEBUG) {
			this.receiver.logMessage("AbstractRTAsyncCall::execute "
									 + this.calledMethodTaskKey + " "
									 + Thread.currentThread());
		}

		SporadicTask st =
				(SporadicTask) this.scheduler.getRealTimeTask(
												this.calledMethodTaskKey);
		AperiodicTaskActivation ata = st.activateNow(parameters, this);
		this.scheduler.addTaskActivationAndDispatch(ata);
	}

	/**
	 * @see fr.sorbonne_u.components.plugins.asynccall.AbstractAsyncCall#sendResult(java.io.Serializable)
	 */
	@Override
	public void			sendResult(Serializable result) throws Exception
	{
		if (DEBUG) {
			this.receiver.logMessage("AbstractRTAsyncCall::sendResult "
									 + this.calledMethodTaskKey + " "
									 + Thread.currentThread());
		}

		super.sendResult(result);
	}
}
// -----------------------------------------------------------------------------
