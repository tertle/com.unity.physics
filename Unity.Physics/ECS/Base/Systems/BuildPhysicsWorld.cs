using Unity.Burst;
using Unity.Burst.Intrinsics;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Entities;
using Unity.Jobs;
using Unity.Jobs.LowLevel.Unsafe;
using Unity.Mathematics;
using Unity.Transforms;
using UnityEngine;
using UnityEngine.Assertions;

#if UNITY_EDITOR
using UnityEditor;
#endif

namespace Unity.Physics.Systems
{
    // Make sure that:
    // 1. BuildPhysicsWorldDependencyResolver is always updated just before BuildPhysicsWorld
    // 2. BuildPhysicsWorld is the last system to be updated in [PhysicsInitializeGroup]
    // This is done to prevent race conditions if users put something to UpdateIn[PhysicsInitializeGroup].
    // They shouldn't be doing so, but it is nice to prevent unresolvable race conditions.
    [UpdateInGroup(typeof(PhysicsInitializeGroup), OrderLast = true)]
    internal partial class PhysicsInitializeGroupInternal : ComponentSystemGroup
    {
    }

    [UpdateInGroup(typeof(PhysicsInitializeGroupInternal), OrderFirst = true)]
    [CreateAfter(typeof(BuildPhysicsWorld))]
    [BurstCompile]
    internal partial struct BuildPhysicsWorldDependencyResolver : ISystem
    {
        [BurstCompile]
        public void OnCreate(ref SystemState state)
        {
            SystemAPI.GetSingletonRW<PhysicsWorldSingleton>();
            SystemAPI.GetSingletonRW<SimulationSingleton>();
        }

        [BurstCompile]
        public void OnUpdate(ref SystemState state)
        {
            ref var buildPhysicsData = ref SystemAPI.GetSingletonRW<BuildPhysicsWorldData>().ValueRW;
            buildPhysicsData.AddInputDependencyToComplete(state.Dependency);
        }
    }

    /// <summary>
    /// Group responsible to build the physics world data and wait for any dependencies before the
    /// next simulation.
    /// </summary>
    [UpdateInGroup(typeof(PhysicsInitializeGroupInternal), OrderLast = true)]
    [UpdateAfter(typeof(BuildPhysicsWorldDependencyResolver))]
    public partial class PhysicsBuildWorldGroup : ComponentSystemGroup
    {
    }

    /// <summary>
    /// Public system data for this world's instance of a <see cref="BuildPhysicsWorld"/> system.
    ///
    /// Contains physics world data based on the entity world. The physics world data will contain a
    /// rigid body for every entity which has a rigid body component, and a joint for every entity
    /// which has a joint component.
    /// </summary>
    public struct BuildPhysicsWorldData : IComponentData
    {
        internal JobHandle m_InputDependencyToComplete;

        /// <summary>
        /// Information describing the <see cref="PhysicsWorldData"/>. Important : avoid using
        /// BuildPhysicsWorldData.PhysicsWorldData.PhysicsWorld. Use <see cref="PhysicsWorldSingleton"/>
        /// instead.
        /// </summary>
        public PhysicsWorldData PhysicsData;

        /// <summary>   A filter specifying the world. </summary>
        public PhysicsWorldIndex WorldFilter;

        /// <summary>   Gets the group the dynamic bodies belongs to. </summary>
        ///
        /// <value> The dynamic entity group. </value>
        public EntityQuery DynamicEntityGroup => this.PhysicsData.DynamicEntityGroup;

        /// <summary>   Gets the group the static bodies belongs to. </summary>
        ///
        /// <value> The static entity group. </value>
        public EntityQuery StaticEntityGroup => this.PhysicsData.StaticEntityGroup;

        /// <summary>   Gets the group the joints belongs to. </summary>
        ///
        /// <value> The joint entity group. </value>
        public EntityQuery JointEntityGroup => this.PhysicsData.JointEntityGroup;

        /// <summary>   True if the static bodies have changed this frame. </summary>
        ///
        /// <value> The have static bodies changed flag. </value>
        public NativeReference<int> HaveStaticBodiesChanged => this.PhysicsData.HaveStaticBodiesChanged;

#if (UNITY_EDITOR || DEVELOPMENT_BUILD) && !UNITY_PHYSICS_DISABLE_INTEGRITY_CHECKS
        internal NativeParallelHashMap<uint, long> IntegrityCheckMap;
#endif

        /// <summary>
        /// Adds the dependency that BuildPhysicsWorld will complete on the next OnUpdate() call.
        /// Multiple dependencies can be added this way (they are combined).
        /// BuildPhysicsWorld resets the PhysicsWorld immediately in the OnUpdate() method (not through jobs),
        /// so any jobs that rely on that data should use this to make sure their data is not ruined before they access it.
        /// </summary>
        internal void AddInputDependencyToComplete(JobHandle dependencyToComplete)
        {
            this.m_InputDependencyToComplete = JobHandle.CombineDependencies(this.m_InputDependencyToComplete, dependencyToComplete);
        }

        /// <summary>
        /// Complete and reset all pending input dependencies.
        /// </summary>
        public void CompleteInputDependency()
        {
            this.m_InputDependencyToComplete.Complete();
            this.m_InputDependencyToComplete = default;
        }
    }

    /// <summary>
    /// A system which builds the physics world based on the entity world. The world will contain a
    /// rigid body for every entity which has a rigid body component, and a joint for every entity
    /// which has a joint component.
    /// </summary>
    [BurstCompile]
    [UpdateInGroup(typeof(PhysicsBuildWorldGroup))]
    public partial struct BuildPhysicsWorld : ISystem
    {
        [BurstCompile]
        public void OnCreate(ref SystemState state)
        {
            var worldFilter = new PhysicsWorldIndex();
            var physicsData = new PhysicsWorldData(ref state, worldFilter);
            state.EntityManager.AddComponentData(state.SystemHandle, new BuildPhysicsWorldData
            {
                WorldFilter = worldFilter,
                PhysicsData = physicsData,
            });

            state.EntityManager.CreateSingleton(new PhysicsWorldSingleton
            {
                PhysicsWorld = physicsData.PhysicsWorld,
                PhysicsWorldIndex = worldFilter,
            });
        }

        [BurstCompile]
        public void OnDestroy(ref SystemState state)
        {
            ref var buildPhysicsData = ref state.EntityManager.GetComponentDataRW<BuildPhysicsWorldData>(state.SystemHandle).ValueRW;
            buildPhysicsData.PhysicsData.Dispose();
        }

        [BurstCompile]
        public void OnUpdate(ref SystemState state)
        {
            ref var buildPhysicsData = ref SystemAPI.GetSingletonRW<BuildPhysicsWorldData>().ValueRW;

            var timeStep = SystemAPI.Time.DeltaTime;

            if (!SystemAPI.TryGetSingleton(out PhysicsStep stepComponent))
            {
                stepComponent = PhysicsStep.Default;
            }

            state.Dependency = JobHandle.CombineDependencies(buildPhysicsData.m_InputDependencyToComplete, state.Dependency);

            state.Dependency = PhysicsWorldBuilder.SchedulePhysicsWorldBuild(ref state, ref buildPhysicsData.PhysicsData, state.Dependency, timeStep,
                stepComponent.CollisionTolerance, stepComponent.MultiThreaded > 0, stepComponent.IncrementalDynamicBroadphase,
                stepComponent.IncrementalStaticBroadphase, stepComponent.Gravity, state.LastSystemVersion);

            SystemAPI.SetSingleton(new PhysicsWorldSingleton
            {
                PhysicsWorld = buildPhysicsData.PhysicsData.PhysicsWorld,
                PhysicsWorldIndex = buildPhysicsData.WorldFilter,
            });
        }
    }

    /// <summary>
    /// System responsible for adding important temporal coherence info components to rigid bodies
    /// when the incremental broadphase is enabled.
    /// </summary>
    [RequireMatchingQueriesForUpdate]
    [UpdateInGroup(typeof(FixedStepSimulationSystemGroup), OrderFirst = true)]
    [UpdateBefore(typeof(BeginFixedStepSimulationEntityCommandBufferSystem))]
    internal partial struct InjectTemporalCoherenceDataSystem : ISystem
    {
        private TemporalCoherenceUtilities.Queries m_Queries;

        [BurstCompile]
        public void OnCreate(ref SystemState state)
        {
            this.m_Queries = TemporalCoherenceUtilities.CreateQueries(ref state);

            state.RequireForUpdate<BeginFixedStepSimulationEntityCommandBufferSystem.Singleton>();
        }

        [BurstCompile]
        public void OnUpdate(ref SystemState state)
        {
            if (!SystemAPI.TryGetSingleton<PhysicsStep>(out var physicsStep))
            {
                physicsStep = PhysicsStep.Default;
            }

            // add temporal coherence data if incremental broadphase is used
            if ((physicsStep.IncrementalDynamicBroadphase || physicsStep.IncrementalStaticBroadphase) && !this.m_Queries.IsEmpty)
            {
                var ecb = SystemAPI.GetSingleton<BeginFixedStepSimulationEntityCommandBufferSystem.Singleton>().CreateCommandBuffer(state.WorldUnmanaged);
                TemporalCoherenceUtilities.AddTemporalCoherenceComponents(ref this.m_Queries, ref ecb);
            }
        }
    }

    /// <summary>
    /// <para>
    /// System responsible for adding important temporal coherence info components to rigid bodies
    /// when the incremental broadphase is enabled.
    /// </para>
    /// <para>
    /// This system is a last resort to add the necessary components to rigid bodies when the data has not yet been
    /// added by the regular injection system InjectTemporalCoherenceDataSystem. This is necessary when rigid bodies
    /// are created after the start of the FixedStepSimulationSystemGroup.
    /// </para>
    /// </summary>
    [RequireMatchingQueriesForUpdate]
    [UpdateInGroup(typeof(PhysicsBuildWorldGroup))]
    [UpdateBefore(typeof(BuildPhysicsWorld))]
    internal partial struct InjectTemporalCoherenceDataLastResortSystem : ISystem, ISystemStartStop
    {
        private TemporalCoherenceUtilities.Queries m_Queries;
        private bool m_First;

        [BurstCompile]
        public void OnCreate(ref SystemState state)
        {
            this.m_Queries = TemporalCoherenceUtilities.CreateQueries(ref state);
            this.m_First = true;
        }

        [BurstCompile]
        public void OnStartRunning(ref SystemState state)
        {
            this.m_First = true;
        }

        [BurstCompile]
        public void OnStopRunning(ref SystemState state)
        {
            this.m_First = true;
        }

        [BurstCompile]
        public void OnUpdate(ref SystemState state)
        {
            if (!SystemAPI.TryGetSingleton<PhysicsStep>(out var physicsStep))
            {
                physicsStep = PhysicsStep.Default;
            }

            // add temporal coherence data if incremental broadphase is used
            if ((physicsStep.IncrementalDynamicBroadphase || physicsStep.IncrementalStaticBroadphase) && !this.m_Queries.IsEmpty)
            {
                if (this.m_First)
                {
                    Debug.LogWarning("InjectTemporalCoherenceDataLastResortSystem has to inject missing PhysicsTemporalCoherenceInfo " +
                        "or PhysicsTemporalCoherenceTag components on rigid body entities with enabled incremental broadphase. " +
                        "If you create rigid body entities following the start of the FixedStepSimulationSystemGroup, make sure they contain " +
                        "all components needed for incremental broadphase at creation time, or create them before the FixedStepSimulationSystemGroup.");

                    this.m_First = false;
                }

                var ecb = new EntityCommandBuffer(Allocator.Temp);
                TemporalCoherenceUtilities.AddTemporalCoherenceComponents(ref this.m_Queries, ref ecb);
                ecb.Playback(state.EntityManager);
            }
        }
    }

    /// <summary>
    /// <para>
    /// System responsible for removing temporal coherence info components from rigid body entities that have been destroyed.
    /// </para>
    /// <para>
    /// This system is part of the incremental broadphase feature.
    /// </para>
    /// </summary>
    [RequireMatchingQueriesForUpdate]
    [UpdateInGroup(typeof(PhysicsBuildWorldGroup))]
    internal partial struct InvalidatedTemporalCoherenceCleanupSystem : ISystem
    {
        private EntityQuery m_InvalidatedTemporalCoherenceCleanupGroup;

        [BurstCompile]
        private partial struct InvalidatedTemporalCoherenceCleanupJob : IJobEntity
        {
            public EntityCommandBuffer.ParallelWriter ECB;

            private void Execute(in Entity entity, [ChunkIndexInQuery] int chunkIndex)
            {
                this.ECB.RemoveComponent<PhysicsTemporalCoherenceInfo>(chunkIndex, entity);
            }
        }

        [BurstCompile]
        public void OnCreate(ref SystemState state)
        {
            state.RequireForUpdate<EndFixedStepSimulationEntityCommandBufferSystem.Singleton>();

            this.m_InvalidatedTemporalCoherenceCleanupGroup = new EntityQueryBuilder(Allocator.Temp)
                .WithAll<PhysicsTemporalCoherenceInfo>()
                .WithAbsent<PhysicsTemporalCoherenceTag>()
                .Build(ref state);

            state.RequireForUpdate(this.m_InvalidatedTemporalCoherenceCleanupGroup);
        }

        [BurstCompile]
        public void OnUpdate(ref SystemState state)
        {
            var ecb = SystemAPI.GetSingleton<EndFixedStepSimulationEntityCommandBufferSystem.Singleton>().CreateCommandBuffer(state.WorldUnmanaged);

            state.Dependency = new InvalidatedTemporalCoherenceCleanupJob
            {
                ECB = ecb.AsParallelWriter(),
            }.ScheduleParallel(this.m_InvalidatedTemporalCoherenceCleanupGroup, state.Dependency);
        }
    }

#region Integrity checks
#if (UNITY_EDITOR || DEVELOPMENT_BUILD) && !UNITY_PHYSICS_DISABLE_INTEGRITY_CHECKS
    /// <summary>
    /// Schedule check for integrity jobs in the Editor or in development build if the
    /// UNITY_PHYSICS_DISABLE_INTEGRITY_CHECKS define is not set.
    /// </summary>
    [BurstCompile]
    [UpdateInGroup(typeof(PhysicsBuildWorldGroup), OrderLast = true)]
    [CreateAfter(typeof(BuildPhysicsWorld))]
    internal partial struct IntegrityCheckSystem : ISystem
    {
        NativeParallelHashMap<uint, long> m_IntegrityCheckMap;
        SystemHandle m_BuildSystemHandle;

        [BurstCompile]
        public void OnCreate(ref SystemState state)
        {
            m_IntegrityCheckMap = new NativeParallelHashMap<uint, long>(4, Allocator.Persistent);
            m_BuildSystemHandle = state.WorldUnmanaged.GetExistingUnmanagedSystem<BuildPhysicsWorld>();
            ref var buildPhysicsWorldData =
                ref state.EntityManager.GetComponentDataRW<BuildPhysicsWorldData>(m_BuildSystemHandle).ValueRW;
            buildPhysicsWorldData.IntegrityCheckMap = m_IntegrityCheckMap;
            // To inject the right dependencies
            state.GetComponentLookup<PhysicsWorldSingleton>();
        }

        public void OnDestroy(ref SystemState state)
        {
            m_IntegrityCheckMap.Dispose();
        }

        [BurstCompile]
        public void OnUpdate(ref SystemState state)
        {
            // these getter are not waiting for depedencies on the main thread, and that would avoid blocking for no
            // reason.
            var buildPhysicsData = SystemAPI.GetSingleton<BuildPhysicsWorldData>();
            state.Dependency = new ClearIntegrityCheckMapJob { IntegrityCheckMap = buildPhysicsData.IntegrityCheckMap }.Schedule(state.Dependency);
            // Should be un-necessary to update the handles at this point (there are no structural changes at this point)
            // has been added as extra safety measure.
            // buildPhysicsData.PhysicsData.ComponentHandles.Update(ref state);
            state.Dependency = new PhysicsIntegrityCheckJobs.RecordDynamicBodyIntegrity
            {
                IntegrityCheckMap = buildPhysicsData.IntegrityCheckMap,
                LocalTransformType = buildPhysicsData.PhysicsData.ComponentHandles.LocalTransformType,
                PhysicsVelocityType = buildPhysicsData.PhysicsData.ComponentHandles.PhysicsVelocityType,
                PhysicsColliderType = buildPhysicsData.PhysicsData.ComponentHandles.PhysicsColliderType
            }.Schedule(buildPhysicsData.PhysicsData.DynamicEntityGroup, state.Dependency);
            state.Dependency = new PhysicsIntegrityCheckJobs.RecordColliderIntegrity
            {
                IntegrityCheckMap = buildPhysicsData.IntegrityCheckMap,
                PhysicsColliderType = buildPhysicsData.PhysicsData.ComponentHandles.PhysicsColliderType
            }.Schedule(buildPhysicsData.PhysicsData.StaticEntityGroup, state.Dependency);;
        }

        [BurstCompile]
        private struct ClearIntegrityCheckMapJob : IJob
        {
            public NativeParallelHashMap<uint, long> IntegrityCheckMap;

            public void Execute()
            {
                this.IntegrityCheckMap.Clear();
            }
        }
    }

    internal static class PhysicsIntegrityCheckJobs
    {
        [BurstCompile]
        internal struct RecordDynamicBodyIntegrity : IJobChunk
        {
            [ReadOnly] public ComponentTypeHandle<LocalTransform> LocalTransformType;

            [ReadOnly] public ComponentTypeHandle<PhysicsVelocity> PhysicsVelocityType;
            [ReadOnly] public ComponentTypeHandle<PhysicsCollider> PhysicsColliderType;

            public NativeParallelHashMap<uint, long> IntegrityCheckMap;

            internal static void AddOrIncrement(NativeParallelHashMap<uint, long> integrityCheckMap,
                uint systemVersion)
            {
                if (integrityCheckMap.TryGetValue(systemVersion, out long occurences))
                {
                    integrityCheckMap.Remove(systemVersion);
                    integrityCheckMap.Add(systemVersion, occurences + 1);
                }
                else
                {
                    integrityCheckMap.Add(systemVersion, 1);
                }
            }

            public void Execute(in ArchetypeChunk chunk, int unfilteredChunkIndex, bool useEnabledMask,
                in v128 chunkEnabledMask)
            {
                Assert.IsFalse(useEnabledMask);
                AddOrIncrement(IntegrityCheckMap, chunk.GetOrderVersion());
                AddOrIncrement(IntegrityCheckMap, chunk.GetChangeVersion(ref PhysicsVelocityType));

                if (chunk.Has(ref LocalTransformType))
                {
                    AddOrIncrement(IntegrityCheckMap, chunk.GetChangeVersion(ref LocalTransformType));
                }

                if (chunk.Has(ref PhysicsColliderType))
                {
                    AddOrIncrement(IntegrityCheckMap, chunk.GetChangeVersion(ref PhysicsColliderType));
                }
            }
        }

        [BurstCompile]
        internal struct RecordColliderIntegrity : IJobChunk
        {
            [ReadOnly] public ComponentTypeHandle<PhysicsCollider> PhysicsColliderType;
            public NativeParallelHashMap<uint, long> IntegrityCheckMap;

            public void Execute(in ArchetypeChunk chunk, int unfilteredChunkIndex, bool useEnabledMask,
                in v128 chunkEnabledMask)
            {
                Assert.IsFalse(useEnabledMask);
                if (chunk.Has(ref PhysicsColliderType))
                {
                    RecordDynamicBodyIntegrity.AddOrIncrement(IntegrityCheckMap,
                        chunk.GetChangeVersion(ref PhysicsColliderType));
                }
            }
        }
    }
#endif
#endregion

#region Analytics
#if UNITY_EDITOR && ENABLE_CLOUD_SERVICES_ANALYTICS
    internal struct PhysicsAnalyticsSingleton : IComponentData
    {
        public SimulationType m_SimulationType;
        public uint m_MaxNumberOfStaticBodiesInAScene;
        public uint m_MaxNumberOfDynamicBodiesInAScene;

        public uint m_MaxNumberOfConvexesInAScene;
        public uint m_MaxNumberOfSpheresInAScene;
        public uint m_MaxNumberOfCapsulesInAScene;
        public uint m_MaxNumberOfTrianglesInAScene;
        public uint m_MaxNumberOfQuadsInAScene;
        public uint m_MaxNumberOfBoxesInAScene;
        public uint m_MaxNumberOfCylindersInAScene;
        public uint m_MaxNumberOfMeshesInAScene;
        public uint m_MaxNumberOfCompoundsInAScene;
        public uint m_MaxNumberOfTerrainsInAScene;

        public uint m_MaxNumberOfLinearConstraintsInAScene;
        public uint m_MaxNumberOfAngularConstraintsInAScene;
        public uint m_MaxNumberOfRotationMotorsInAScene;
        public uint m_MaxNumberOfAngularVelocityMotorsInAScene;
        public uint m_MaxNumberOfPositionMotorsInAScene;
        public uint m_MaxNumberOfLinearVelocityMotorsInAScene;

        public void Clear()
        {
            this.m_SimulationType = SimulationType.NoPhysics;
            this.m_MaxNumberOfStaticBodiesInAScene = 0;
            this.m_MaxNumberOfDynamicBodiesInAScene = 0;
            this.m_MaxNumberOfConvexesInAScene = 0;
            this.m_MaxNumberOfSpheresInAScene = 0;
            this.m_MaxNumberOfCapsulesInAScene = 0;
            this.m_MaxNumberOfTrianglesInAScene = 0;
            this.m_MaxNumberOfQuadsInAScene = 0;
            this.m_MaxNumberOfBoxesInAScene = 0;
            this.m_MaxNumberOfCylindersInAScene = 0;
            this.m_MaxNumberOfMeshesInAScene = 0;
            this.m_MaxNumberOfCompoundsInAScene = 0;
            this.m_MaxNumberOfTerrainsInAScene = 0;
            this.m_MaxNumberOfLinearConstraintsInAScene = 0;
            this.m_MaxNumberOfAngularConstraintsInAScene = 0;
            this.m_MaxNumberOfRotationMotorsInAScene = 0;
            this.m_MaxNumberOfAngularVelocityMotorsInAScene = 0;
            this.m_MaxNumberOfPositionMotorsInAScene = 0;
            this.m_MaxNumberOfLinearVelocityMotorsInAScene = 0;
        }
    }

    [RequireMatchingQueriesForUpdate]
    [UpdateInGroup(typeof(PhysicsInitializeGroupInternal), OrderLast = true)]
    [UpdateAfter(typeof(PhysicsBuildWorldGroup))]
    internal partial struct PhysicsAnalyticsSystem : ISystem
    {
        public void OnCreate(ref SystemState state)
        {
            if (!EditorAnalytics.enabled)
            {
                return;
            }

            var physicsAnalyticsSingleton = new PhysicsAnalyticsSingleton();
            physicsAnalyticsSingleton.Clear();
            state.EntityManager.CreateSingleton(physicsAnalyticsSingleton);

            // To inject the right dependencies
            state.GetComponentLookup<PhysicsWorldSingleton>();
        }

        public void OnUpdate(ref SystemState state)
        {
            if (!EditorAnalytics.enabled)
            {
                return;
            }

            if (!SystemAPI.TryGetSingleton(out PhysicsStep stepComponent))
            {
                stepComponent = PhysicsStep.Default;
            }

            // Store simulation type
            var analyticsData = SystemAPI.GetSingletonRW<PhysicsAnalyticsSingleton>();
            analyticsData.ValueRW.m_SimulationType = stepComponent.SimulationType;

            var buildPhysicsData = SystemAPI.GetSingleton<BuildPhysicsWorldData>();
            var physicsWorld = buildPhysicsData.PhysicsData.PhysicsWorld;

            // store the max number of static and dynamic bodies in the scene
            analyticsData.ValueRW.m_MaxNumberOfStaticBodiesInAScene = math.max(
                analyticsData.ValueRO.m_MaxNumberOfStaticBodiesInAScene, (uint)physicsWorld.NumStaticBodies);

            analyticsData.ValueRW.m_MaxNumberOfDynamicBodiesInAScene = math.max(
                analyticsData.ValueRO.m_MaxNumberOfDynamicBodiesInAScene, (uint)physicsWorld.NumDynamicBodies);

            if (physicsWorld.NumJoints == 0 && physicsWorld.NumBodies <= 1)
            {
                return;
            }
            // else:

            var maxThreadCount = JobsUtility.ThreadIndexCount;

            var rigidBodyAnalyticsData = CollectionHelper.CreateNativeArray<PhysicsAnalyticsSingleton>(maxThreadCount,
                state.WorldUpdateAllocator, NativeArrayOptions.ClearMemory);

            var jointAnalyticsData = CollectionHelper.CreateNativeArray<PhysicsAnalyticsSingleton>(maxThreadCount,
                state.WorldUpdateAllocator, NativeArrayOptions.ClearMemory);

            JobHandle jointAnalyticsJob = default;
            if (physicsWorld.NumJoints > 0)
            {
                jointAnalyticsJob = new AnalyticsJobs.ParallelJointAnalyticsJob
                {
                    JointAnalyticsData = jointAnalyticsData,
                    Joints = physicsWorld.Joints,
                }.Schedule(physicsWorld.NumJoints, 32, state.Dependency);
            }

            JobHandle bodiesAnalyticsJob = default;
            // Check if we have more bodies than the default static body
            if (physicsWorld.NumBodies > 1)
            {
                bodiesAnalyticsJob = new AnalyticsJobs.ParallelRigidBodyAnalyticsJob
                {
                    RigidBodyAnalyticsData = rigidBodyAnalyticsData,
                    RigidBodies = physicsWorld.Bodies,
                }.Schedule(physicsWorld.NumBodies, 32, state.Dependency);
            }

            var gatherAnalyticsJobHandle = JobHandle.CombineDependencies(jointAnalyticsJob, bodiesAnalyticsJob);

            state.Dependency = new AnalyticsJobs.FinalizePhysicsWorldAnalyticsJob
            {
                RigidBodyAnalyticsData = rigidBodyAnalyticsData,
                JointAnalyticsData = jointAnalyticsData,
                PhysicsAnalyticsSingleton = analyticsData,
            }.Schedule(gatherAnalyticsJobHandle);
        }
    }

    internal static class AnalyticsJobs
    {
        [BurstCompile]
        internal struct ParallelJointAnalyticsJob : IJobParallelFor
        {
            [NativeSetThreadIndex]
            private int m_ThreadIndex;
            [NativeDisableParallelForRestriction]
            public NativeArray<PhysicsAnalyticsSingleton> JointAnalyticsData;
            [ReadOnly]
            public NativeArray<Joint> Joints;

            public void Execute(int index)
            {
                var data = this.JointAnalyticsData[this.m_ThreadIndex];

                var joint = this.Joints[index];
                var constraints = joint.Constraints.GetConstraints();

                foreach (var constraint in constraints)
                {
                    switch (constraint.Type)
                    {
                        case ConstraintType.Linear:
                            ++data.m_MaxNumberOfLinearConstraintsInAScene;
                            break;
                        case ConstraintType.Angular:
                            ++data.m_MaxNumberOfAngularConstraintsInAScene;
                            break;
                        case ConstraintType.PositionMotor:
                            ++data.m_MaxNumberOfPositionMotorsInAScene;
                            break;
                        case ConstraintType.RotationMotor:
                            ++data.m_MaxNumberOfRotationMotorsInAScene;
                            break;
                        case ConstraintType.AngularVelocityMotor:
                            ++data.m_MaxNumberOfAngularVelocityMotorsInAScene;
                            break;
                        case ConstraintType.LinearVelocityMotor:
                            ++data.m_MaxNumberOfLinearVelocityMotorsInAScene;
                            break;
                    }
                }

                this.JointAnalyticsData[this.m_ThreadIndex] = data;
            }
        }

        [BurstCompile]
        internal struct ParallelRigidBodyAnalyticsJob : IJobParallelFor
        {
            [NativeSetThreadIndex]
            private int m_ThreadIndex;
            [NativeDisableParallelForRestriction]
            public NativeArray<PhysicsAnalyticsSingleton> RigidBodyAnalyticsData;
            [ReadOnly]
            public NativeArray<RigidBody> RigidBodies;

            public void Execute(int index)
            {
                var body = this.RigidBodies[index];
                if (!body.Collider.IsCreated)
                {
                    return;
                }
                // else:

                var data = this.RigidBodyAnalyticsData[this.m_ThreadIndex];

                switch (body.Collider.Value.Type)
                {
                    case ColliderType.Convex:
                        ++data.m_MaxNumberOfConvexesInAScene;
                        break;
                    case ColliderType.Sphere:
                        ++data.m_MaxNumberOfSpheresInAScene;
                        break;
                    case ColliderType.Capsule:
                        ++data.m_MaxNumberOfCapsulesInAScene;
                        break;
                    case ColliderType.Triangle:
                        ++data.m_MaxNumberOfSpheresInAScene;
                        break;
                    case ColliderType.Quad:
                        ++data.m_MaxNumberOfQuadsInAScene;
                        break;
                    case ColliderType.Box:
                        ++data.m_MaxNumberOfBoxesInAScene;
                        break;
                    case ColliderType.Cylinder:
                        ++data.m_MaxNumberOfCylindersInAScene;
                        break;
                    case ColliderType.Mesh:
                        ++data.m_MaxNumberOfMeshesInAScene;
                        break;
                    case ColliderType.Compound:
                        ++data.m_MaxNumberOfCompoundsInAScene;
                        break;
                    case ColliderType.Terrain:
                        ++data.m_MaxNumberOfTerrainsInAScene;
                        break;
                }

                this.RigidBodyAnalyticsData[this.m_ThreadIndex] = data;
            }
        }

        [BurstCompile]
        internal struct FinalizePhysicsWorldAnalyticsJob : IJob
        {
            [ReadOnly]
            public NativeArray<PhysicsAnalyticsSingleton> JointAnalyticsData;
            [ReadOnly]
            public NativeArray<PhysicsAnalyticsSingleton> RigidBodyAnalyticsData;
            [NativeDisableUnsafePtrRestriction]
            public RefRW<PhysicsAnalyticsSingleton> PhysicsAnalyticsSingleton;

            public void Execute()
            {
                PhysicsAnalyticsSingleton sumData = default;

                // include rigid body data
                foreach (var data in this.RigidBodyAnalyticsData)
                {
                    // sum up the collected data from all threads
                    sumData.m_MaxNumberOfConvexesInAScene += data.m_MaxNumberOfConvexesInAScene;
                    sumData.m_MaxNumberOfSpheresInAScene += data.m_MaxNumberOfSpheresInAScene;
                    sumData.m_MaxNumberOfCapsulesInAScene += data.m_MaxNumberOfCapsulesInAScene;
                    sumData.m_MaxNumberOfTrianglesInAScene += data.m_MaxNumberOfTrianglesInAScene;
                    sumData.m_MaxNumberOfQuadsInAScene += data.m_MaxNumberOfQuadsInAScene;
                    sumData.m_MaxNumberOfBoxesInAScene += data.m_MaxNumberOfBoxesInAScene;
                    sumData.m_MaxNumberOfCylindersInAScene += data.m_MaxNumberOfCylindersInAScene;
                    sumData.m_MaxNumberOfMeshesInAScene += data.m_MaxNumberOfMeshesInAScene;
                    sumData.m_MaxNumberOfCompoundsInAScene += data.m_MaxNumberOfCompoundsInAScene;
                    sumData.m_MaxNumberOfTerrainsInAScene += data.m_MaxNumberOfTerrainsInAScene;
                }

                // include joint data
                foreach (var data in this.JointAnalyticsData)
                {
                    // sum up the collected data from all threads
                    sumData.m_MaxNumberOfLinearConstraintsInAScene += data.m_MaxNumberOfLinearConstraintsInAScene;
                    sumData.m_MaxNumberOfAngularConstraintsInAScene += data.m_MaxNumberOfAngularConstraintsInAScene;
                    sumData.m_MaxNumberOfRotationMotorsInAScene += data.m_MaxNumberOfRotationMotorsInAScene;
                    sumData.m_MaxNumberOfAngularVelocityMotorsInAScene += data.m_MaxNumberOfAngularVelocityMotorsInAScene;
                    sumData.m_MaxNumberOfPositionMotorsInAScene += data.m_MaxNumberOfPositionMotorsInAScene;
                    sumData.m_MaxNumberOfLinearVelocityMotorsInAScene += data.m_MaxNumberOfLinearVelocityMotorsInAScene;
                }

                // obtain the last known data record and update the maximum values:
                var maxData = this.PhysicsAnalyticsSingleton.ValueRO;

                // rigid body data
                maxData.m_MaxNumberOfConvexesInAScene = math.max(maxData.m_MaxNumberOfConvexesInAScene, sumData.m_MaxNumberOfConvexesInAScene);
                maxData.m_MaxNumberOfSpheresInAScene = math.max(maxData.m_MaxNumberOfSpheresInAScene, sumData.m_MaxNumberOfSpheresInAScene);
                maxData.m_MaxNumberOfCapsulesInAScene = math.max(maxData.m_MaxNumberOfCapsulesInAScene, sumData.m_MaxNumberOfCapsulesInAScene);
                maxData.m_MaxNumberOfTrianglesInAScene = math.max(maxData.m_MaxNumberOfTrianglesInAScene, sumData.m_MaxNumberOfTrianglesInAScene);
                maxData.m_MaxNumberOfQuadsInAScene = math.max(maxData.m_MaxNumberOfQuadsInAScene, sumData.m_MaxNumberOfQuadsInAScene);
                maxData.m_MaxNumberOfBoxesInAScene = math.max(maxData.m_MaxNumberOfBoxesInAScene, sumData.m_MaxNumberOfBoxesInAScene);
                maxData.m_MaxNumberOfCylindersInAScene = math.max(maxData.m_MaxNumberOfCylindersInAScene, sumData.m_MaxNumberOfCylindersInAScene);
                maxData.m_MaxNumberOfMeshesInAScene = math.max(maxData.m_MaxNumberOfMeshesInAScene, sumData.m_MaxNumberOfMeshesInAScene);
                maxData.m_MaxNumberOfCompoundsInAScene = math.max(maxData.m_MaxNumberOfCompoundsInAScene, sumData.m_MaxNumberOfCompoundsInAScene);
                maxData.m_MaxNumberOfTerrainsInAScene = math.max(maxData.m_MaxNumberOfTerrainsInAScene, sumData.m_MaxNumberOfTerrainsInAScene);

                // joint data
                maxData.m_MaxNumberOfLinearConstraintsInAScene =
                    math.max(maxData.m_MaxNumberOfLinearConstraintsInAScene, sumData.m_MaxNumberOfLinearConstraintsInAScene);

                maxData.m_MaxNumberOfAngularConstraintsInAScene =
                    math.max(maxData.m_MaxNumberOfAngularConstraintsInAScene, sumData.m_MaxNumberOfAngularConstraintsInAScene);

                maxData.m_MaxNumberOfRotationMotorsInAScene =
                    math.max(maxData.m_MaxNumberOfRotationMotorsInAScene, sumData.m_MaxNumberOfRotationMotorsInAScene);

                maxData.m_MaxNumberOfAngularVelocityMotorsInAScene = math.max(maxData.m_MaxNumberOfAngularVelocityMotorsInAScene,
                    sumData.m_MaxNumberOfAngularVelocityMotorsInAScene);

                maxData.m_MaxNumberOfPositionMotorsInAScene =
                    math.max(maxData.m_MaxNumberOfPositionMotorsInAScene, sumData.m_MaxNumberOfPositionMotorsInAScene);

                maxData.m_MaxNumberOfLinearVelocityMotorsInAScene = math.max(maxData.m_MaxNumberOfLinearVelocityMotorsInAScene,
                    sumData.m_MaxNumberOfLinearVelocityMotorsInAScene);

                // write the data back into the component
                this.PhysicsAnalyticsSingleton.ValueRW = maxData;
            }
        }
    }

#endif //UNITY_EDITOR && ENABLE_CLOUD_SERVICES_ANALYTICS
#endregion

    internal struct DummySimulationData : IComponentData
    {
        private byte dummyData; // Without data, the component is zero-sized, and accessing it will result in error
        internal DummySimulation m_Simulation;

        internal void DisableSystemChain(ref SystemState systemStateRef)
        {
            systemStateRef.Enabled = false;
            this.m_Simulation.Dispose();
        }

        internal void EnableSystemChain(ref SystemState systemStateRef)
        {
            systemStateRef.Enabled = true;
            this.m_Simulation = new DummySimulation();
        }
    }

    [UpdateInGroup(typeof(PhysicsSimulationGroup))]
    [BurstCompile]
    internal partial struct DummySimulationSystem : ISystem
    {
        [BurstCompile]
        public void OnCreate(ref SystemState state)
        {
            state.EntityManager.AddComponentData(state.SystemHandle, new DummySimulationData
            {
                // This one is enabled by default
                m_Simulation = new DummySimulation(),
            });
        }

        [BurstCompile]
        public void OnDestroy(ref SystemState state)
        {
            state.EntityManager.GetComponentDataRW<DummySimulationData>(state.SystemHandle).ValueRW.m_Simulation.Dispose();
        }

        [BurstCompile]
        public void OnUpdate(ref SystemState state)
        {
            // do nothing;
        }
    }
}
