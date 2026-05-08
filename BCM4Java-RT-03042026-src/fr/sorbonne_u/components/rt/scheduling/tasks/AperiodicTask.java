package fr.sorbonne_u.components.rt.scheduling.tasks;

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

import fr.sorbonne_u.components.exceptions.BCMRuntimeException;
import fr.sorbonne_u.components.rt.AbstractRTComponent;
import fr.sorbonne_u.components.rt.annotations.AperiodicTaskDescription;
import fr.sorbonne_u.components.rt.asynccall.AbstractRTAsyncCall;
import fr.sorbonne_u.components.rt.scheduling.SchedulingI;
import fr.sorbonne_u.exceptions.AssertionChecking;
import fr.sorbonne_u.exceptions.ImplementationInvariantException;
import fr.sorbonne_u.exceptions.InvariantException;
import fr.sorbonne_u.exceptions.PreconditionException;
import java.lang.reflect.Method;

// -----------------------------------------------------------------------------
/**
 * The class <code>AperiodicTask</code> implements a real-time aperiodic task
 * that is executed by calling a service method (properly annotated with
 * {@code AperiodicTaskDescription}).
 *
 * <p><strong>Description</strong></p>
 * 
 * <p>
 * The basic and most standard way to use aperiodic tasks in a real time
 * component is to activate it upon the occurrence of an external call from
 * another component (real-time or not) that must be treated by the task. As
 * the call is external, it is usually impossible to have a lowest bound on
 * the delay between two successive activations.
 * </p>
 * <p>
 * Often, such an external call will require the return of a result. With this
 * implementation, it is possible to activate an aperiodic task and get a
 * completable future from its {@code AperiodicTaskActivation}, which will be
 * completed by the task when it will be available. Hence, for example, another
 * component or an internal  periodic task activation may start a computation
 * as an aperiodic task activation and have one of its future activation look
 * at the completable future to get the result when it will be known. In a
 * real-time component, looking for such a future result should never block
 * the task (no {@code get}). The usual pattern is either to call a non blocking
 * test, for example {@code getNow} or {@code isDone}, and be prepared to do
 * something else if the value is not available yet.
 * </p>
 * 
 * <p><strong>Implementation Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code method.isAnnotationPresent(AperiodicTaskDescription.class)}
 * invariant	{@code maxNumberOfActivationsPerTimeUnit > 0.0}
 * invariant	{@code numberOfActivationsInLastPeriod >= 0}
 * invariant	{@code lastReferencePeriodStartTime > 0}
 * </pre>
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
public class			AperiodicTask
extends		AbstractAperiodicTask
{
	// -------------------------------------------------------------------------
	// Constants and variables
	// -------------------------------------------------------------------------

	/** maximum number of activations per inherited {@code timeUnit}.		*/
	protected final double	maxNumberOfActivationsPerTimeUnit;
	/** number of activations of this task in the last period of one
	 *  inherited time unit.												*/
	protected int			numberOfActivationsInLastPeriod;
	/** last reference period absolute start time in scheduler time unit
	 *  to control the number of activations. 								*/
	protected long			lastReferencePeriodStartTime;

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
		AperiodicTask instance
		)
	{
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		ret &= AssertionChecking.checkImplementationInvariant(
				instance.method.isAnnotationPresent(
											AperiodicTaskDescription.class),
				AperiodicTask.class,
				instance,
				"method.isAnnotationPresent(AperiodicTaskDescription.class)");
		ret &= AssertionChecking.checkImplementationInvariant(
				instance.maxNumberOfActivationsPerTimeUnit > 0,
				AperiodicTask.class,
				instance,
				"maxNumberOfActivationsPerTimeUnit > 0");
		ret &= AssertionChecking.checkImplementationInvariant(
				instance.numberOfActivationsInLastPeriod >= 0,
				AperiodicTask.class,
				instance,
				"numberOfActivationsInLastPeriod >= 0");
		ret &= AssertionChecking.checkImplementationInvariant(
				instance.lastReferencePeriodStartTime > 0,
				AperiodicTask.class,
				instance,
				"lastReferencePeriodStartTime > 0");
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
		AperiodicTask instance
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
	 * create a new aperiodic real time task.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code owner != null}
	 * pre	{@code scheduler != null}
	 * pre	{@code method != null}
	 * pre	{@code method.getDeclaringClass().isAssignableFrom(owner.getClass())}
	 * pre	{@code method.isAnnotationPresent(AperiodicTaskDescription.class)}
	 * post	{@code getNumber() == method.getAnnotation(AperiodicTaskDescription.class).no()}
	 * post	{@code getTimeUnit() == method.getAnnotation(AperiodicTaskDescription.class).timeUnit()}
	 * post	{@code getWcet() == method.getAnnotation(AperiodicTaskDescription.class).wcet()}
	 * post	{@code getRelativeDeadline() == method.getAnnotation(AperiodicTaskDescription.class).relativeDeadline()}
	 * post	{@code getMutexTasks() == null || getMutexTasks().equals(method.getAnnotation(AperiodicTaskDescription.class).mutexTasks())}
	 * post	{@code getName().equals(method.getName())}
	 * post	{@code isVarArgs() == method.isVarArgs()}
	 * post	{@code getNumberOfParameters() == method.getParameterCount()}
	 * post	{@code getMaxNumberOfActivationsPerTimeUnit() == method.getAnnotation(AperiodicTaskDescription.class).maxNumberOfActivationsPerTimeUnit()}
	 * </pre>
	 *
	 * @param owner			real time component holding the code this task require to execute.
	 * @param scheduler		scheduler responsible to trigger the execution of the task activations; associated with the owner component.
	 * @param method		the method in the owner component that this task will execute.
	 * @throws Exception	when {@code method == null} or the {@code AperiodicTaskDescription} is not present on it.
	 */
	public				AperiodicTask(
		AbstractRTComponent owner,
		SchedulingI scheduler,
		Method method
		) throws Exception
	{
		super(owner, 
			  scheduler,
			  AssertionChecking.assertTrueAndReturnOrThrow(
					method != null &&
						method.isAnnotationPresent(AperiodicTaskDescription.class),
					method,
					() -> new PreconditionException("method != null")).
				getAnnotation(AperiodicTaskDescription.class).no(),
			  method.getAnnotation(AperiodicTaskDescription.class).timeUnit(),
			  method.getAnnotation(AperiodicTaskDescription.class).wcet(),
			  method.getAnnotation(AperiodicTaskDescription.class).relativeDeadline(),
			  method.getAnnotation(AperiodicTaskDescription.class).mutexTasks(),
			  method);

		this.maxNumberOfActivationsPerTimeUnit =
				method.getAnnotation(AperiodicTaskDescription.class).
										maxNumberOfActivationsPerTimeUnit();

		assert 	AperiodicTask.implementationInvariants(this) :
				new ImplementationInvariantException(
						"AperiodicTask.implementationInvariants(this)");
		assert	AperiodicTask.invariants(this) :
				new InvariantException("AperiodicTask.invariants(this)");
	}

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
	 * pre	{@code owner.getClass().isAssignableFrom(method.getDeclaringClass())}
	 * pre	{@code method.isAnnotationPresent(AperiodicTaskDescription.class)}
	 * pre	{@code maxNumberOfActivationsPerTimeUnit > 0.0}
	 * post	{@code getNumber() == no}
	 * post	{@code getTimeUnit() == timeUnit}
	 * post	{@code getWcet() == wcet}
	 * post	{@code getRelativeDeadline() == relativeDeadline}
	 * post	{@code getMutexTasks() == null || getMutexTasks().equals(mutexTasks)}
	 * post	{@code getName().equals(method.getName())}
	 * post	{@code isVarArgs() == method.isVarArgs()}
	 * post	{@code getNumberOfParameters() == method.getParameterCount()}
	 * post	{@code getMaxNumberOfActivationsPerTimeUnit() == maxNumberOfActivationsPerTimeUnit}
	 * </pre>
	 *
	 * @param owner								real time component holding the code this task require to execute.
	 * @param scheduler							scheduler responsible to trigger the execution of the task activations; associated with the owner component.
	 * @param no								unique task number given by the user to this task.
	 * @param timeUnit							time unit in which the times are expressed in this descriptor.
	 * @param wcet								worst-case execution time of the task.
	 * @param relativeDeadline					deadline for the task; for periodic tasks, this is relative to the scheduling cycle start time.
	 * @param mutexTasks						numbers of the tasks that must execute in mutual exclusion with this one.
	 * @param method							the method in the owner component that this task will execute.
	 * @param maxNumberOfActivationsPerTimeUnit maximum number of activations per {@code timeUnit}.
	 */
	public				AperiodicTask(
		AbstractRTComponent owner,
		SchedulingI scheduler,
		int no,
		TimeUnit timeUnit,
		long wcet,
		long relativeDeadline,
		int[] mutexTasks,
		Method method,
		double maxNumberOfActivationsPerTimeUnit
		)
	{
		super(owner, scheduler, no, timeUnit, wcet, relativeDeadline, mutexTasks,
			  method);

		assert	maxNumberOfActivationsPerTimeUnit > 0.0 :
				new PreconditionException(
						"maxNumberOfActivationsPerTimeUnit > 0.0");

		this.maxNumberOfActivationsPerTimeUnit =
											maxNumberOfActivationsPerTimeUnit;
		this.numberOfActivationsInLastPeriod = 0;
		this.lastReferencePeriodStartTime = -1;

		assert 	AperiodicTask.implementationInvariants(this) :
				new ImplementationInvariantException(
						"AperiodicTask.implementationInvariants(this)");
		assert	AperiodicTask.invariants(this) :
				new InvariantException("AperiodicTask.invariants(this)");
	}

	// -------------------------------------------------------------------------
	// Methods
	// -------------------------------------------------------------------------

	/**
	 * return	the maximum number of activations per time unit.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return > 0.0}
	 * </pre>
	 *
	 * @return	the maximum number of activations per time unit.
	 */
	public double	getMaxNumberOfActivationsPerTimeUnit()
	{
		return this.maxNumberOfActivationsPerTimeUnit;
	}

	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.tasks.AbstractRealTimeTask#activate(long, java.lang.Object[])
	 */
	@Override
	public AperiodicTaskActivation	activate(
		long activationTime,
		Object[] params
		)
	{
		return this.activate(activationTime, params, null);
	}

	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.tasks.AbstractAperiodicTask#activate(long, java.lang.Object[], fr.sorbonne_u.components.rt.asynccall.AbstractRTAsyncCall)
	 */
	@Override
	public AperiodicTaskActivation activate(
		long activationTime,
		Object[] params,
		AbstractRTAsyncCall resultSendingProxy
		)
	{
		assert	activationTime > 0 : 
				new PreconditionException("activationTime > 0");

		if (this.lastReferencePeriodStartTime < 0) {
			this.lastReferencePeriodStartTime = activationTime;
		}

		assert 	activationTime > this.lastReferencePeriodStartTime :
			new RuntimeException(
					"activationTime > this.lastReferencePeriodStartTime");

		long period =
			this.scheduler.convertToSchedulerTimeUnit(1, this.getTimeUnit());
		if (activationTime >= this.lastReferencePeriodStartTime + period) {
			this.lastReferencePeriodStartTime = activationTime;
			this.numberOfActivationsInLastPeriod = 1;
		} else {
			this.numberOfActivationsInLastPeriod++;
			assert	this.numberOfActivationsInLastPeriod >
										this.maxNumberOfActivationsPerTimeUnit :
					new BCMRuntimeException(
							"this.numberOfActivationsInLastPeriod > "
							+ "this.maxNumberOfActivationsPerTimeUnit");
		}

		assert 	AperiodicTask.implementationInvariants(this) :
				new ImplementationInvariantException(
						"AperiodicTask.implementationInvariants(this)");
		assert	AperiodicTask.invariants(this) :
				new InvariantException("AperiodicTask.invariants(this)");

		return new AperiodicTaskActivation(
								this, this.scheduler, activationTime, params,
								resultSendingProxy);
	}

	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.tasks.AbstractRealTimeTask#toStringContent(java.lang.StringBuffer)
	 */
	@Override
	protected void		toStringContent(StringBuffer sb)
	{
		super.toStringContent(sb);
		sb.append("; maxNumberOfActivationsPerTimeUnit = ");
		sb.append(this.maxNumberOfActivationsPerTimeUnit);
	}
}
// -----------------------------------------------------------------------------
