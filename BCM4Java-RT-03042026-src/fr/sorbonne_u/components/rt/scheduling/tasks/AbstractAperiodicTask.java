package fr.sorbonne_u.components.rt.scheduling.tasks;

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

import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import java.io.Serializable;
import fr.sorbonne_u.components.exceptions.BCMRuntimeException;
import fr.sorbonne_u.components.rt.AbstractRTComponent;
import fr.sorbonne_u.components.rt.asynccall.AbstractRTAsyncCall;
import fr.sorbonne_u.components.rt.scheduling.SchedulingI;
import fr.sorbonne_u.exceptions.PreconditionException;

// -----------------------------------------------------------------------------
/**
 * The abstract class <code>AbstractAperiodicTask</code> defines the
 * complementary common information describing an aperiodic real time task for
 * a real time component.
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
 * <p>Created on : 2025-06-03</p>
 * 
 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
 */
public abstract class	AbstractAperiodicTask
extends		AbstractRealTimeTask
{
	// -------------------------------------------------------------------------
	// Constants and variables
	// -------------------------------------------------------------------------

	/** when true, the scheduler makes debugging traces of its actions
	 *  through the owner component tracing facility.						*/
	public static boolean	DEBUG = false;

	// -------------------------------------------------------------------------
	// Constructors
	// -------------------------------------------------------------------------

	/**
	 * create a new aperiodic real time task.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code owner != null}
	 * pre	{@code scheduler != null}
	 * pre	{@code no >= 0}
	 * pre	{@code timeUnit != null}
	 * pre	{@code wcet > 0}
	 * pre	{@code relativeDeadline >= wcet}
	 * pre	{@code mutexTasks == null || no > 0}
	 * pre	{@code mutexTasks == null || Arrays.stream(mutexTasks).allMatch(n -> n > 0 && n != no)}
	 * pre	{@code method != null}
	 * pre	{@code method.getDeclaringClass().isAssignableFrom(owner.getClass())}
	 * post	{@code getNumber() == no}
	 * post	{@code getTimeUnit() == timeUnit}
	 * post	{@code getWcet() == wcet}
	 * post	{@code getRelativeDeadline() == relativeDeadline}
	 * post	{@code getMutexTasks() == null || getMutexTasks().equals(mutexTasks)}
	 * post	{@code getName().equals(method.getName())}
	 * post	{@code isVarArgs() == method.isVarArgs()}
	 * post	{@code getNumberOfParameters() == method.getParameterCount()}
	 * </pre>
	 *
	 * @param owner				real time component holding the code this task require to execute.
	 * @param scheduler			scheduler responsible to trigger the execution of the task activations; associated with the owner component.
	 * @param no				unique task number given by the user to this task.
	 * @param timeUnit			time unit in which the times are expressed in this descriptor.
	 * @param wcet				worst-case execution time of the task.
	 * @param relativeDeadline	deadline for the task; for aperiodic tasks, this is relative to the task activation time.
	 * @param mutexTasks		numbers of the tasks that must execute in mutual exclusion with this one.
	 * @param method			the method in the owner component that this task will execute.
	 */
	public				AbstractAperiodicTask(
		AbstractRTComponent owner,
		SchedulingI scheduler,
		int no,
		TimeUnit timeUnit,
		long wcet,
		long relativeDeadline,
		int[] mutexTasks,
		Method method
		)
	{
		super(owner, scheduler, no, timeUnit, wcet, relativeDeadline,
			  mutexTasks, method);
	}

	// -------------------------------------------------------------------------
	// Methods
	// -------------------------------------------------------------------------

	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.tasks.AbstractRealTimeTask#activateNow(java.lang.Object[])
	 */
	@Override
	public AperiodicTaskActivation	activateNow(Object[] params)
	{
		return this.activateNow(params, null);
	}

	/**
	 * activate the aperiodic task now with a proxy to send the result back to
	 * the caller.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param params				parameters of the task.
	 * @param resultSendingProxy	proxy to send the result back to the caller.
	 * @return						a new aperiodic task activation.
	 */
	public AperiodicTaskActivation	activateNow(
		Object[] params,
		AbstractRTAsyncCall	resultSendingProxy
		)
	{
		long activationTime =
				this.scheduler.convertToSchedulerTimeUnit(
											System.currentTimeMillis(),
											TimeUnit.MILLISECONDS);
		return this.activate(activationTime, params, resultSendingProxy);
	}

	/**
	 * activate the aperiodic task at the given real time with a proxy to
	 * send the result back to the caller.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code activationTime >= 0}
	 * post	{@code ret != null}
	 * </pre>
	 *
	 * @param activationTime		the real time of activation of the task in the scheduler time unit.
	 * @param params				parameters of the task.
	 * @param resultSendingProxy	proxy to send the result back to the caller.
	 * @return						a new aperiodic task activation.
	 */
	public abstract AperiodicTaskActivation	activate(
		long activationTime,
		Object[] params,
		AbstractRTAsyncCall	resultSendingProxy
		);

	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.tasks.AbstractRealTimeTask#execute(java.lang.Object[])
	 */
	@Override
	public Serializable 	execute(Object[] params) throws Exception
	{
		if (DEBUG) {
			System.out.println("AbstractAperiodicTask::execute "
					   		   + this.getName() + " " + Thread.currentThread());
		}

		assert	params != null : new PreconditionException("params != null");
		Object ret = this.method.invoke(this.owner, params);
		assert	ret == null || ret instanceof Serializable :
				new BCMRuntimeException("ret instanceof Serializable");
		return (Serializable) ret;
	}
}
// -----------------------------------------------------------------------------
