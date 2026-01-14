using Unity.Burst;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Jobs;

namespace Unity.Physics
{
    /// <summary>   Processes body pairs and creates contacts from them. </summary>
    static class NarrowPhase
    {
        /// <summary>
        /// Iterates the provided dispatch pairs and creates contacts and based on them.
        /// </summary>
        ///
        /// <param name="world">                [in,out] The world. </param>
        /// <param name="inputVelocities">      [in] The velocity prediction at the end of the frame. </param>
        /// <param name="dispatchPairs">        The dispatch pairs. </param>
        /// <param name="solverSchedulerInfo">  The solver scheduler info. </param>
        /// <param name="timeStep">             The time step for the full frame. </param>
        /// <param name="contactsWriter">       [in,out] The contacts writer. </param>
        internal static void CreateContacts(ref PhysicsWorld world, NativeArray<Velocity> inputVelocities,
            NativeArray<DispatchPairSequencer.DispatchPair> dispatchPairs,
            ref DispatchPairSequencer.SolverSchedulerInfo solverSchedulerInfo, float timeStep,
            ref NativeStream.Writer contactsWriter)
        {
            // pure iterative pairs:
            if (solverSchedulerInfo.IterativePairsIterativeScheduling.NumDispatchPairs.Value > 0)
            {
                ParallelCreateContactsJob.ExecuteImpl(ref world, inputVelocities, timeStep, dispatchPairs,
                    solverSchedulerInfo.IterativePairsIterativeScheduling.FirstDispatchPairIndex.Value,
                    solverSchedulerInfo.IterativePairsIterativeScheduling.NumDispatchPairs.Value, ref contactsWriter,
                    solverSchedulerInfo.IterativePairsIterativeScheduling.FirstWorkItemIndex.Value);
            }

            // iterative coupling pairs:
            if (solverSchedulerInfo.CouplingPairsIterativeScheduling.NumDispatchPairs.Value > 0)
            {
                ParallelCreateContactsJob.ExecuteImpl(ref world, inputVelocities, timeStep, dispatchPairs,
                    solverSchedulerInfo.CouplingPairsIterativeScheduling.FirstDispatchPairIndex.Value,
                    solverSchedulerInfo.CouplingPairsIterativeScheduling.NumDispatchPairs.Value, ref contactsWriter,
                    solverSchedulerInfo.CouplingPairsIterativeScheduling.FirstWorkItemIndex.Value);
            }

            // direct pairs:
            if (solverSchedulerInfo.DirectPairsIterativeScheduling.NumDispatchPairs.Value > 0)
            {
                ParallelCreateContactsJob.ExecuteImpl(ref world, inputVelocities, timeStep, dispatchPairs,
                    solverSchedulerInfo.DirectPairsIterativeScheduling.FirstDispatchPairIndex.Value,
                    solverSchedulerInfo.DirectPairsIterativeScheduling.NumDispatchPairs.Value, ref contactsWriter,
                    solverSchedulerInfo.DirectPairsIterativeScheduling.FirstWorkItemIndex.Value);
            }
        }

        /// <summary>
        /// Schedules a set of jobs to iterate the provided dispatch pairs and create contacts based on
        /// them.
        /// </summary>
        ///
        /// <param name="world">                [in,out] The physics world. </param>
        /// <param name="timeStep">             The full frame time step. </param>
        /// <param name="inputVelocities">      A NativeArray of Linear and Angular velocities expected at end of the frame if only gravity integration is considered. </param>
        /// <param name="contacts">             [in,out] The contacts. </param>
        /// <param name="jacobians">            [in,out] The jacobians. </param>
        /// <param name="dispatchPairs">        [in,out] The dispatch pairs. </param>
        /// <param name="inputDeps">            The input deps. </param>
        /// <param name="solverSchedulerInfo"> [in,out] Information describing the solver scheduler. </param>
        /// <param name="multiThreaded">        (Optional) True if multi threaded. </param>
        ///
        /// <returns>   The SimulationJobHandles. </returns>
        internal static SimulationJobHandles ScheduleCreateContactsJobs(ref PhysicsWorld world, float timeStep,
            NativeArray<Velocity> inputVelocities, ref NativeStream contacts, ref NativeStream jacobians,
            ref NativeList<DispatchPairSequencer.DispatchPair> dispatchPairs, JobHandle inputDeps,
            ref DispatchPairSequencer.SolverSchedulerInfo solverSchedulerInfo, bool multiThreaded = true)
        {
            SimulationJobHandles returnHandles = default;

            var numWorkItems = solverSchedulerInfo.NumIterativeWorkItems;
            var contactsHandle = NativeStream.ScheduleConstruct(out contacts, numWorkItems, inputDeps, Allocator.TempJob);
            var jacobiansHandle = NativeStream.ScheduleConstruct(out jacobians, numWorkItems, inputDeps, Allocator.TempJob);
            var streamConstructionHandle = JobHandle.CombineDependencies(contactsHandle, jacobiansHandle);

            if (!multiThreaded)
            {
                returnHandles.FinalExecutionHandle = new CreateContactsJob
                {
                    World = world,
                    InputVelocities = inputVelocities,
                    TimeStep = timeStep,
                    DispatchPairs = dispatchPairs.AsDeferredJobArray(),
                    SolverSchedulerInfo = solverSchedulerInfo,
                    ContactsWriter = contacts.AsWriter()
                }.Schedule(streamConstructionHandle);
            }
            else
            {
                var createContactsJobIterative = new ParallelCreateContactsJob
                {
                    World = world,
                    InputVelocities = inputVelocities,
                    TimeStep = timeStep,
                    DispatchPairs = dispatchPairs.AsDeferredJobArray(),
                    IterativeSolverSchedulerInfo = solverSchedulerInfo.IterativePairsIterativeScheduling,
                    ContactsWriter = contacts.AsWriter()
                }.ScheduleUnsafeIndex0(solverSchedulerInfo.IterativePairsIterativeScheduling.NumWorkItems, 1, streamConstructionHandle);

                var createContactsJobIterativeCoupling = new ParallelCreateContactsJob
                {
                    World = world,
                    InputVelocities = inputVelocities,
                    TimeStep = timeStep,
                    DispatchPairs = dispatchPairs.AsDeferredJobArray(),
                    IterativeSolverSchedulerInfo = solverSchedulerInfo.CouplingPairsIterativeScheduling,
                    ContactsWriter = contacts.AsWriter()
                }.ScheduleUnsafeIndex0(solverSchedulerInfo.CouplingPairsIterativeScheduling.NumWorkItems, 1, streamConstructionHandle);

                var createContactsJobIterativeDirect = new ParallelCreateContactsJob
                {
                    World = world,
                    InputVelocities = inputVelocities,
                    TimeStep = timeStep,
                    DispatchPairs = dispatchPairs.AsDeferredJobArray(),
                    IterativeSolverSchedulerInfo = solverSchedulerInfo.DirectPairsIterativeScheduling,
                    ContactsWriter = contacts.AsWriter()
                }.ScheduleUnsafeIndex0(solverSchedulerInfo.DirectPairsIterativeScheduling.NumWorkItems, 1, streamConstructionHandle);

                returnHandles.FinalExecutionHandle = JobHandle.CombineDependencies(createContactsJobIterative,
                    createContactsJobIterativeCoupling, createContactsJobIterativeDirect);
            }

            return returnHandles;
        }

        [BurstCompile]
        [NoAlias]
        struct ParallelCreateContactsJob : IJobParallelForDefer
        {
            [NoAlias, ReadOnly] public PhysicsWorld World;
            [ReadOnly] public float TimeStep; //Full frame timestep
            [ReadOnly] public NativeArray<Velocity> InputVelocities;
            [ReadOnly] public NativeArray<DispatchPairSequencer.DispatchPair> DispatchPairs;
            [NoAlias, NativeDisableContainerSafetyRestriction] public NativeStream.Writer ContactsWriter;
            [NoAlias, ReadOnly] public DispatchPairSequencer.IterativeSolverSchedulerInfo IterativeSolverSchedulerInfo;

            public void Execute(int workItemIndexOffset)
            {
                var firstDispatchPairIndex = IterativeSolverSchedulerInfo.FirstDispatchPairIndex.Value
                    + IterativeSolverSchedulerInfo.GetWorkItemReadOffset(workItemIndexOffset, out int numPairsToRead);
                var workItemIndex = IterativeSolverSchedulerInfo.FirstWorkItemIndex.Value + workItemIndexOffset;

                ExecuteImpl(ref World, InputVelocities, TimeStep, DispatchPairs, firstDispatchPairIndex, numPairsToRead,
                    ref ContactsWriter, workItemIndex);
            }

            // Note: timestep needs to be for full frame
            internal static void ExecuteImpl(ref PhysicsWorld world, NativeArray<Velocity> inputVelocities,
                float timeStep, NativeArray<DispatchPairSequencer.DispatchPair> dispatchPairs,
                int firstDispatchPairIndex, int numDispatchPairs, ref NativeStream.Writer contactWriter, int workItemIndex)
            {
                contactWriter.BeginForEachIndex(workItemIndex);

                for (int i = 0; i < numDispatchPairs; i++)
                {
                    DispatchPairSequencer.DispatchPair dispatchPair = dispatchPairs[firstDispatchPairIndex + i];

                    // Invalid pairs can exist by being disabled by users
                    if (dispatchPair.IsValid)
                    {
                        if (dispatchPair.IsContact)
                        {
                            // Create contact manifolds for this pair of bodies
                            var pair = new BodyIndexPair
                            {
                                BodyIndexA = dispatchPair.BodyIndexA,
                                BodyIndexB = dispatchPair.BodyIndexB
                            };

                            RigidBody rigidBodyA = world.Bodies[pair.BodyIndexA];
                            RigidBody rigidBodyB = world.Bodies[pair.BodyIndexB];

                            MotionVelocity motionVelocityA;
                            if (pair.BodyIndexA < world.MotionVelocities.Length)
                            {
                                var motionVelocity = world.MotionVelocities[pair.BodyIndexA];
                                motionVelocityA = new MotionVelocity()
                                {
                                    InverseInertia = motionVelocity.InverseInertia,
                                    InverseMass = motionVelocity.InverseMass,
                                    AngularExpansionFactor = motionVelocity.AngularExpansionFactor,
                                    GravityFactor = motionVelocity.GravityFactor,

                                    AngularVelocity = inputVelocities[pair.BodyIndexA].Angular,
                                    LinearVelocity = inputVelocities[pair.BodyIndexA].Linear // end frame velocity prediction
                                };
                            }
                            else
                            {
                                motionVelocityA = MotionVelocity.Zero;
                            }

                            MotionVelocity motionVelocityB;
                            if (pair.BodyIndexB < world.MotionVelocities.Length)
                            {
                                var motionVelocity = world.MotionVelocities[pair.BodyIndexB];
                                motionVelocityB = new MotionVelocity()
                                {
                                    InverseInertia = motionVelocity.InverseInertia,
                                    InverseMass = motionVelocity.InverseMass,
                                    AngularExpansionFactor = motionVelocity.AngularExpansionFactor,
                                    GravityFactor = motionVelocity.GravityFactor,

                                    AngularVelocity = inputVelocities[pair.BodyIndexB].Angular,
                                    LinearVelocity = inputVelocities[pair.BodyIndexB].Linear // end frame velocity prediction
                                };
                            }
                            else
                            {
                                motionVelocityB = MotionVelocity.Zero;
                            }

                            ManifoldQueries.BodyBody(rigidBodyA, rigidBodyB, motionVelocityA, motionVelocityB,
                                world.CollisionWorld.CollisionTolerance, timeStep, pair, ref contactWriter);
                        }
                    }
                }

                contactWriter.EndForEachIndex();
            }
        }

        [BurstCompile]
        [NoAlias]
        struct CreateContactsJob : IJob
        {
            [NoAlias, ReadOnly] public PhysicsWorld World;
            [ReadOnly] public float TimeStep; //Full frame timestep
            [ReadOnly] public NativeArray<Velocity> InputVelocities;
            [ReadOnly] public NativeArray<DispatchPairSequencer.DispatchPair> DispatchPairs;
            [ReadOnly] public DispatchPairSequencer.SolverSchedulerInfo SolverSchedulerInfo;
            [NoAlias] public NativeStream.Writer ContactsWriter;

            public void Execute()
            {
                CreateContacts(ref World, InputVelocities, DispatchPairs, ref SolverSchedulerInfo, TimeStep, ref ContactsWriter);
            }
        }
    }
}
