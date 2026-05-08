package fr.sorbonne_u.components.rt.annotations;

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

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

// -----------------------------------------------------------------------------
/**
 * The annotation <code>SporadicTaskDescription</code> defines information
 * needed to describe a sporadic task for real time scheduling.
 *
 * <p><strong>Description</strong></p>
 * 
 * <p>
 * A sporadic task is a real-time aperiodic task that has a worst-case execution
 * time, a minimal delay between two successive activations, and a deadline.
 * All times and durations are given in {@code timeUnit()}. When mutual
 * exclusion constraints exist among tasks scheduled together, numbers can be
 * given to tasks using {@code no()} and {@code mutex()} returns an array of
 * the numbers of the tasks that must be executed in mutual exclusion with this
 * one.
 * </p>
 * 
 * <p><strong>Implementation Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code true}	// no invariant
 * </pre>
 * 
 * <p><strong>Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code relativeDeadline() >= wcet()}
 * invariant	{@code minimumInterActivationDelay() > 0}
 * invariant	{@code mutexTasks() == null || no() > 0 || }
 * invariant	{@code mutexTasks() == null || Arrays.stream(mutexTasks()).allMatch(n -> n > 0 && n != no())}
 * </pre>
 * 
 * <p>Created on : 2021-02-26</p>
 * 
 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
 */
@Retention(RUNTIME)
@Target(METHOD)
public @interface		SporadicTaskDescription
{
	/**
	 * the locally (within a component) unique number identifying the task;
	 * a 0 number is considered as non numbered task.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return >= 0}
	 * </pre>
	 *
	 * @return	the locally (within a component) unique number identifying the task; a 0 number is considered as non numbered task.
	 */
	public int			no() default 0;

	/**
	 * time unit used in expressing times and durations in this annotation.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return != null}
	 * </pre>
	 *
	 * @return	the time unit used in expressing times and durations in this annotation.
	 */
	public TimeUnit		timeUnit();

	/**
	 * worst-case execution time of the task in {@code timeUnit()}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return > 0}
	 * </pre>
	 *
	 * @return	the worst-case execution time of the task in {@code timeUnit()}.
	 */
	public long			wcet();

	/**
	 * maximal delay in {@code timeUnit()} to finish the task after its
	 * current activation time.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return >= wcet()}
	 * </pre>
	 *
	 * @return	the maximal delay in {@code timeUnit()} to finish the task after its current activation time.
	 */
	public long			relativeDeadline();

	/**
	 * minimal delay in {@code timeUnit()} between successive activations of
	 * the task.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return > 0}
	 * </pre>
	 *
	 * @return	the minimal delay in {@code timeUnit()} between successive activations of the task.
	 */
	public long			minimumInterActivationDelay() default Long.MAX_VALUE;

	/**
	 * array of task numbers that must be executed in mutual exclusion with
	 * this one defaults to the empty array, which denotes the absence of
	 * mutual exclusion constraint for the task.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return != null && Arrays.stream(return).allMatch(n -> n > 0 && n != no())}
	 * </pre>
	 *
	 * @return	an array of task numbers that must be executed in mutual exclusion with this one.
	 */
	public int[]		mutexTasks() default {};
}
// -----------------------------------------------------------------------------
