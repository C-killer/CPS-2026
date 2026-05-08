package fr.sorbonne_u.components.rt.scheduling;

// Copyright Jacques Malenfant, Sorbonne Universite.
// Jacques.Malenfant@lip6.fr
//
// This software is a computer program whose purpose is to provide a
// basic component programming model to program with components
// real time distributed applications in the Java programming language.
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

import java.util.concurrent.TimeUnit;
import fr.sorbonne_u.components.rt.AbstractRTComponent;
import fr.sorbonne_u.components.rt.scheduling.tasks.AbstractRealTimeTaskActivation;
import fr.sorbonne_u.components.rt.scheduling.tasks.AbstractRealTimeTask;
import java.lang.reflect.Method;

// -----------------------------------------------------------------------------
/**
 * The interface <code>SchedulingI</code> declares the methods to be implemented
 * by a scheduler used in a real time component.
 *
 * <p><strong>Description</strong></p>
 * 
 * <p><strong>Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * <p>Created on : 2021-03-03</p>
 * 
 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
 */
public interface		SchedulingI
{
	// -------------------------------------------------------------------------
	// Inner types and classes
	// -------------------------------------------------------------------------

	/**
	 * The interface <code>TaskKeyI</code> tags all task key classes as
	 * legitimate to be handled as such by the framework.
	 *
	 * <p><strong>Description</strong></p>
	 * 
	 * <p><strong>Invariants</strong></p>
	 * 
	 * <pre>
	 * invariant	{@code true}	// no more invariant
	 * </pre>
	 * 
	 * <p>Created on : 2023-04-07</p>
	 * 
	 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
	 */
	public interface	TaskKeyI	{}

	// -------------------------------------------------------------------------
	// Constants and variables
	// -------------------------------------------------------------------------

	/** shortest delay between the call to <code>startScheduling</code>
	 *  and the value of its parameter <code>realTimeOfStart</code>.		*/
	public static final long	START_DELAY_TOLERANCE = 10L;

	// -------------------------------------------------------------------------
	// Signatures
	// -------------------------------------------------------------------------

	/**
	 * return the component owning this scheduler.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return != null}
	 * </pre>
	 *
	 * @return	the component owning this scheduler.
	 */
	public AbstractRTComponent	getOwner();

	/**
	 * log the message using the owner component logging facility.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param message	message to be logged.
	 */
	public void			logMessage(String message);

	/**
	 * return the time unit used when expressing times and durations.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return != null}
	 * </pre>
	 *
	 * @return	the time unit used when expressing times and durations.
	 */
	public TimeUnit		getSchedulingTimeUnit();

	/**
	 * convert {@code sourceDuration} expressed in {@code sourceTimeUnit} into
	 * a duration expressed in the scheduler time unit.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code sourceDuration >= 0}
	 * pre	{@code sourceTimeUnit != null}
	 * post	{@code return >= 0}
	 * </pre>
	 *
	 * @param sourceDuration	duration expressed in {@code sourceTimeUnit}.
	 * @param sourceTimeUnit	time unit in which {@code sourceDuration} is expressed.
	 * @return					the same duration expressed in the scheduler time unit.
	 */
	public long			convertToSchedulerTimeUnit(
		long sourceDuration,
		TimeUnit sourceTimeUnit
		);

	/**
	 * start the scheduler.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code realTimeOfStart >= System.currentTimeMillis() + START_DELAY_TOLERANCE}
	 * post	{@code !stopped()}
	 * </pre>
	 *
	 * @param realTimeOfStart	real time at which the scheduling must start, expressed in {@code getSchedulingTimeUnit()}.
	 */
	public void			startScheduling(long realTimeOfStart);

	/**
	 * stop the scheduler.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code !stopped()}
	 * post	{@code stopped()}
	 * </pre>
	 *
	 */
	public void			stopScheduling();

	/**
	 * return true if the scheduler is stopped.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @return	true if the scheduler is stopped.
	 */
	public boolean		stopped();

	/**
	 * return true if the task corresponding to {@code method} appears in the
	 * scheduler.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param method	a method.
	 * @return			true if the task corresponding to {@code method} appears in the scheduler.
	 */
	public boolean		taskExists(Method method);

	/**
	 * return true if the task corresponding to {@code method} appears in the
	 * scheduler.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code key != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param key	a task key.
	 * @return		true if a task corresponding to {@code key} appears in the scheduler.
	 */
	public boolean		taskExists(TaskKeyI key);

	/**
	 * create a periodic task in the scheduler.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code method != null}
	 * pre	{@code method.isAnnotationPresent(PeriodicTaskDescription.class)}
	 * pre	{@code method.getDeclaringClass().isAssignableFrom(getOwner().getClass())}
	 * pre	{@code !taskExist(method)}
	 * post	{@code taskExist(method)}
	 * </pre>
	 *
	 * @param method		the method in the owner component that this task will execute.
	 * @throws Exception	when {@code method == null} or the {@code PeriodicTaskDescription} is not present on it.
	 */
	public void			createPeriodicTask(Method method) throws Exception;

	/**
	 * create a sporadic task in the scheduler.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code method != null}
	 * pre	{@code method.isAnnotationPresent(AperiodicTaskDescription.class)}
	 * pre	{@code method.getDeclaringClass().isAssignableFrom(getOwner().getClass())}
	 * pre	{@code !taskExist(method)}
	 * post	{@code taskExist(method)}
	 * </pre>
	 *
	 * @param method		the method in the owner component that this task will execute.
	 * @throws Exception	when {@code method == null} or the {@code AperiodicTaskDescription} is not present on it.
	 */
	public void			createAperiodicTask(Method method) throws Exception;

	/**
	 * create a sporadic task in the scheduler.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code method != null}
	 * pre	{@code method.isAnnotationPresent(SporadicTaskDescription.class)}
	 * pre	{@code method.getDeclaringClass().isAssignableFrom(getOwner().getClass())}
	 * pre	{@code !taskExist(method)}
	 * post	{@code taskExist(method)}
	 * </pre>
	 *
	 * @param method		the method in the owner component that this task will execute.
	 * @throws Exception	when {@code method == null} or the {@code SporadicTaskDescription} is not present on it.
	 */
	public void			createSporadicTask(Method method) throws Exception;

	/**
	 * return the real time task corresponding to {@code tk} or null
	 * if none is found.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code tk != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param tk	a task key to find the corresponding aperiodic or sporadic task.
	 * @return		the real time task corresponding to {@code tk} or null if none is found.
	 */
	public AbstractRealTimeTask	getRealTimeTask(TaskKeyI tk);

	/**
	 * add a real time task activation to be scheduled.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code rta != null && taskExist(rta.getTaskKey()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param rta	a real time task activation to be scheduled.
	 */
	public void			addTaskActivation(
		AbstractRealTimeTaskActivation rta
		);

	/**
	 * add a real time task activation to be scheduled and trigger an immediate
	 * dispatch of the next task activation to be executed.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code rta != null && taskExist(rta.getTaskKey()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param rta	a real time task activation to be scheduled.
	 */
	public void			addTaskActivationAndDispatch(
		AbstractRealTimeTaskActivation rta
		);
}
// -----------------------------------------------------------------------------
