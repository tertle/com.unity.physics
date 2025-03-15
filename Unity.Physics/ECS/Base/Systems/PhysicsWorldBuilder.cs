using Unity.Burst;
using Unity.Burst.Intrinsics;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Entities;
using Unity.Jobs;
using Unity.Mathematics;
using Unity.Transforms;
using UnityEngine.Assertions;
using Unity.Physics.Extensions;

namespace Unity.Physics.Systems
{
    /// <summary>   Utilities for building a physics world. </summary>
    [BurstCompile]
    public static class PhysicsWorldBuilder
    {
        internal static JobHandle SchedulePhysicsWorldBuild(
            ref SystemState systemState, ref PhysicsWorldData physicsData, in JobHandle inputDep, float timeStep, float collisionTolerance,
            bool isBroadphaseBuildMultiThreaded, bool isDynamicBroadphaseIncremental, bool isStaticBroadphaseIncremental, float3 gravity,
            uint lastSystemVersion)
        {
            physicsData.Update(ref systemState);
            return SchedulePhysicsWorldBuild(ref systemState, ref physicsData.PhysicsWorld, ref physicsData.HaveStaticBodiesChanged,
                physicsData.ComponentHandles, inputDep, timeStep, collisionTolerance, isBroadphaseBuildMultiThreaded, isDynamicBroadphaseIncremental,
                isStaticBroadphaseIncremental, gravity, lastSystemVersion, physicsData.DynamicEntityGroup, physicsData.StaticEntityGroup,
                physicsData.JointEntityGroup, physicsData.InvalidatedTemporalCoherenceInfoGroup);
        }

        private static JobHandle SchedulePhysicsWorldBuild(
            ref SystemState systemState, ref PhysicsWorld world, ref NativeReference<int> haveStaticBodiesChanged,
            in PhysicsWorldData.PhysicsWorldComponentHandles componentHandles, JobHandle inputDep, float timeStep, float collisionTolerance,
            bool isBroadphaseBuildMultiThreaded, bool isDynamicBroadphaseIncremental, bool isStaticBroadphaseIncremental, float3 gravity,
            uint lastSystemVersion, EntityQuery dynamicEntityQuery, EntityQuery staticEntityQuery, EntityQuery jointEntityQuery,
            EntityQuery invalidatedTemporalCoherenceInfoQuery)
        {
            var numDynamicBodies = dynamicEntityQuery.CalculateEntityCount();
            var numStaticBodies = staticEntityQuery.CalculateEntityCount();
            var numJoints = jointEntityQuery.CalculateEntityCount();

            var previousStaticBodyCount = world.NumStaticBodies - 1; // -1 because of default static body
            var previousDynamicBodyCount = world.NumDynamicBodies;

            // Early out if world is empty and it's been like that in previous frame as well (it contained only the default static body)
            if (numDynamicBodies + numStaticBodies == 0 && world.NumBodies == 1)
            {
                // No bodies in the scene, no need to do anything else
                return new SetChangedJob
                {
                    haveStaticBodiesChanged = haveStaticBodiesChanged,
                    Value = 0,
                }.Schedule(inputDep);
            }

            var rebuildStatics = false;
            var rebuildDynamics = false;

            // If num of dynamics change, need to full rebuild because all static indices will have changed anyway
            // TODO if we switch dynamic/static in body array we can actually avoid rebuilding statics on dynamic structural changes
            if (previousDynamicBodyCount != numDynamicBodies || previousStaticBodyCount != numStaticBodies)
            {
                rebuildStatics = true;
            }

            if (!rebuildStatics)
            {
                // entity count might be the same but orders could still change if so we still need a full rebuild we just don't need to resize
                staticEntityQuery.SetOrderVersionFilter();
                rebuildStatics = !staticEntityQuery.IsEmpty;
                staticEntityQuery.ResetFilter();
            }

            // Can early out here if rebuild statics going to trigger hashmap rebuild anyway
            if (!rebuildStatics)
            {
                // entity count might be the same but orders could still change if so we still need a full rebuild we just don't need to resize
                dynamicEntityQuery.SetOrderVersionFilter();
                rebuildDynamics = !dynamicEntityQuery.IsEmpty;
                dynamicEntityQuery.ResetFilter();
            }

            if (rebuildStatics || rebuildDynamics)
            {
                inputDep.Complete();
                world.CollisionWorld.Reset(numStaticBodies + 1, // +1 for the default static body
                    numDynamicBodies);
            }

            world.DynamicsWorld.Reset(numDynamicBodies, numJoints);

            // Set the desired collision tolerance
            world.CollisionWorld.CollisionTolerance = collisionTolerance;

            // Determine if the static bodies have changed in any way that will require the static broadphase tree to be rebuilt
            JobHandle staticBodiesCheckHandle = default;

            inputDep = new SetChangedJob
            {
                haveStaticBodiesChanged = haveStaticBodiesChanged,
                Value = rebuildStatics ? 1 : 0,
            }.Schedule(inputDep);

            using (var jobHandles = new NativeList<JobHandle>(16, Allocator.Temp))
            {
                NativeArray<int> dynamicBodyChunkBaseEntityIndices = default;
                NativeArray<int> staticBodyChunkBaseEntityIndices = default;

                // Static body changes check jobs
                jobHandles.Add(staticBodiesCheckHandle);

                // Create the default static body at the end of the body list
                // TODO: could skip this if no joints present
                jobHandles.Add(new Jobs.CreateDefaultStaticRigidBody
                {
                    NativeBodies = world.Bodies,
                    BodyIndex = world.Bodies.Length - 1,
                    EntityBodyIndexMap = world.CollisionWorld.EntityBodyIndexMap,
                }.Schedule(inputDep));

                // Dynamic bodies.
                // Create these separately from static bodies to maintain a 1:1 mapping
                // between dynamic bodies and their motions.
                if (numDynamicBodies > 0)
                {
                    // Since these two jobs are scheduled against the same query, they can share a single entity index array.
                    dynamicBodyChunkBaseEntityIndices =
                        dynamicEntityQuery.CalculateBaseEntityIndexArrayAsync(systemState.WorldUpdateAllocator, inputDep, out var baseIndexJob);

                    var createBodiesJob = new Jobs.CreateCreateRigidBodiesFullJob
                    {
                        CreateRigidBodies = new Jobs.CreateRigidBodies
                        {
                            EntityType = componentHandles.EntityType,
                            LocalToWorldType = componentHandles.LocalToWorldType,
                            ParentType = componentHandles.ParentType,
                            LocalTransformType = componentHandles.LocalTransformType,
                            PhysicsColliderType = componentHandles.PhysicsColliderType,
                            PhysicsCustomTagsType = componentHandles.PhysicsCustomTagsType,
                            FirstBodyIndex = 0,
                            RigidBodies = world.Bodies,
                            EntityBodyIndexMap = world.CollisionWorld.EntityBodyIndexMap,
                            ChunkBaseEntityIndices = dynamicBodyChunkBaseEntityIndices,
                        },
                    }.ScheduleParallel(dynamicEntityQuery, baseIndexJob);

                    jobHandles.Add(createBodiesJob);

                    var createMotionsJob = new Jobs.CreateMotions
                    {
                        LocalTransformType = componentHandles.LocalTransformType,
                        PhysicsVelocityType = componentHandles.PhysicsVelocityType,
                        PhysicsMassType = componentHandles.PhysicsMassType,
                        PhysicsMassOverrideType = componentHandles.PhysicsMassOverrideType,
                        PhysicsDampingType = componentHandles.PhysicsDampingType,
                        PhysicsGravityFactorType = componentHandles.PhysicsGravityFactorType,
                        SimulateType = componentHandles.SimulateType,
                        MotionDatas = world.MotionDatas,
                        MotionVelocities = world.MotionVelocities,
                        ChunkBaseEntityIndices = dynamicBodyChunkBaseEntityIndices,
                    }.ScheduleParallel(dynamicEntityQuery, baseIndexJob);

                    jobHandles.Add(createMotionsJob);
                }

                // Now, schedule creation of static bodies, with FirstBodyIndex pointing after
                // the dynamic and kinematic bodies
                if (numStaticBodies > 0)
                {
                    staticBodyChunkBaseEntityIndices =
                        staticEntityQuery.CalculateBaseEntityIndexArrayAsync(systemState.WorldUpdateAllocator, inputDep, out var baseIndexJob);

                    var impl = new Jobs.CreateRigidBodies
                    {
                        EntityType = componentHandles.EntityType,
                        LocalToWorldType = componentHandles.LocalToWorldType,
                        ParentType = componentHandles.ParentType,
                        LocalTransformType = componentHandles.LocalTransformType,
                        PhysicsColliderType = componentHandles.PhysicsColliderType,
                        PhysicsCustomTagsType = componentHandles.PhysicsCustomTagsType,
                        FirstBodyIndex = numDynamicBodies,
                        RigidBodies = world.Bodies,
                        EntityBodyIndexMap = world.CollisionWorld.EntityBodyIndexMap,
                        ChunkBaseEntityIndices = staticBodyChunkBaseEntityIndices,
                    };

                    if (rebuildStatics)
                    {
                        var createBodiesJob =
                            new Jobs.CreateCreateRigidBodiesFullJob { CreateRigidBodies = impl }.ScheduleParallel(staticEntityQuery, baseIndexJob);

                        jobHandles.Add(createBodiesJob);
                    }
                    else
                    {
                        var createBodiesJob = new Jobs.CreateCreateRigidBodiesPartialJob
                        {
                            CreateRigidBodies = impl,
                            LastSystemVersion = lastSystemVersion,
                            Changed = haveStaticBodiesChanged,
                        }.ScheduleParallel(staticEntityQuery, baseIndexJob);

                        jobHandles.Add(createBodiesJob);
                    }
                }

                var combinedHandle = JobHandle.CombineDependencies(jobHandles.AsArray());
                jobHandles.Clear();

                if (rebuildStatics || rebuildDynamics)
                {
                    combinedHandle = new Jobs.RebuildHashMap
                    {
                        EntityBodyIndexMap = world.CollisionWorld.EntityBodyIndexMap,
                        Length = numStaticBodies + numDynamicBodies + 1, // +1 for the default static body
                    }.Schedule(combinedHandle);
                }

                // Build joints
                if (numJoints > 0)
                {
                    var chunkBaseEntityIndices =
                        jointEntityQuery.CalculateBaseEntityIndexArrayAsync(systemState.WorldUpdateAllocator, combinedHandle, out var baseIndexJob);

                    var createJointsJob = new Jobs.CreateJoints
                    {
                        ConstrainedBodyPairComponentType = componentHandles.PhysicsConstrainedBodyPairType,
                        JointComponentType = componentHandles.PhysicsJointType,
                        EntityType = componentHandles.EntityType,
                        Joints = world.Joints,
                        DefaultStaticBodyIndex = world.Bodies.Length - 1,
                        NumDynamicBodies = numDynamicBodies,
                        EntityBodyIndexMap = world.CollisionWorld.EntityBodyIndexMap,
                        EntityJointIndexMap = world.DynamicsWorld.EntityJointIndexMap.AsParallelWriter(),
                        ChunkBaseEntityIndices = chunkBaseEntityIndices,
                    }.ScheduleParallel(jointEntityQuery, baseIndexJob);

                    jobHandles.Add(createJointsJob);
                }

                var buildBroadphaseHandle = world.CollisionWorld.ScheduleBuildBroadphaseJobs(ref world, timeStep, gravity, numDynamicBodies, numStaticBodies,
                    haveStaticBodiesChanged, dynamicEntityQuery, staticEntityQuery, invalidatedTemporalCoherenceInfoQuery, dynamicBodyChunkBaseEntityIndices,
                    staticBodyChunkBaseEntityIndices, combinedHandle, systemState.WorldUpdateAllocator, componentHandles, lastSystemVersion,
                    isBroadphaseBuildMultiThreaded, isDynamicBroadphaseIncremental, isStaticBroadphaseIncremental);

                jobHandles.Add(buildBroadphaseHandle);

                return JobHandle.CombineDependencies(inputDep, JobHandle.CombineDependencies(jobHandles.AsArray()));
            }
        }

#region Jobs
        [BurstCompile]
        private struct SetChangedJob : IJob
        {
            public NativeReference<int> haveStaticBodiesChanged;
            public int Value;

            public void Execute()
            {
                this.haveStaticBodiesChanged.Value = this.Value;
            }
        }

        [BurstCompile]
        private static unsafe class Jobs
        {
            [BurstCompile]
            internal struct CreateDefaultStaticRigidBody : IJob
            {
                [NativeDisableContainerSafetyRestriction]
                public NativeArray<RigidBody> NativeBodies;
                public int BodyIndex;

                [NativeDisableContainerSafetyRestriction]
                public NativeHashMap<Entity, int> EntityBodyIndexMap;

                [BurstCompile]
                public void Execute()
                {
                    this.NativeBodies[this.BodyIndex] = new RigidBody
                    {
                        WorldFromBody = new RigidTransform(quaternion.identity, float3.zero),
                        Scale = 1.0f,
                        Collider = default,
                        Entity = Entity.Null,
                        CustomTags = 0,
                    };

                    var buffer = this.EntityBodyIndexMap.m_Data;
                    var keys = buffer->Keys;
                    var values = (int*)buffer->Ptr;

                    keys[this.BodyIndex] = Entity.Null;
                    values[this.BodyIndex] = this.BodyIndex;
                }
            }

            internal struct CreateRigidBodies
            {
                [ReadOnly]
                public EntityTypeHandle EntityType;
                [ReadOnly]
                public ComponentTypeHandle<LocalToWorld> LocalToWorldType;
                [ReadOnly]
                public ComponentTypeHandle<Parent> ParentType;
                [ReadOnly]
                public ComponentTypeHandle<LocalTransform> LocalTransformType;
                [ReadOnly]
                public ComponentTypeHandle<PhysicsCollider> PhysicsColliderType;
                [ReadOnly]
                public ComponentTypeHandle<PhysicsCustomTags> PhysicsCustomTagsType;
                [ReadOnly]
                public int FirstBodyIndex;

                [NativeDisableContainerSafetyRestriction]
                public NativeArray<RigidBody> RigidBodies;
                [NativeDisableContainerSafetyRestriction]
                public NativeHashMap<Entity, int> EntityBodyIndexMap;
                [ReadOnly]
                public NativeArray<int> ChunkBaseEntityIndices;

                public void Execute(in ArchetypeChunk chunk, int unfilteredChunkIndex, bool useEnabledMask, in v128 chunkEnabledMask)
                {
                    var firstEntityIndexInQuery = this.ChunkBaseEntityIndices[unfilteredChunkIndex];
                    var chunkColliders = chunk.GetNativeArray(ref this.PhysicsColliderType);
                    var chunkLocalToWorlds = chunk.GetNativeArray(ref this.LocalToWorldType);
                    var chunkLocalTransforms = chunk.GetNativeArray(ref this.LocalTransformType);
                    var chunkEntities = chunk.GetNativeArray(this.EntityType);
                    var chunkCustomTags = chunk.GetNativeArray(ref this.PhysicsCustomTagsType);

                    var hasChunkPhysicsColliderType = chunkColliders.IsCreated;
                    var hasChunkPhysicsCustomTagsType = chunk.Has(ref this.PhysicsCustomTagsType);
                    var hasChunkParentType = chunk.Has(ref this.ParentType);
                    var hasChunkLocalToWorldType = chunkLocalToWorlds.IsCreated;
                    var hasChunkLocalTransformType = chunkLocalTransforms.IsCreated;

                    var buffer = this.EntityBodyIndexMap.m_Data;
                    var keys = buffer->Keys;
                    var values = (int*)buffer->Ptr;

                    var worldFromBody = RigidTransform.identity;
                    var entityEnumerator = new ChunkEntityEnumerator(useEnabledMask, chunkEnabledMask, chunk.Count);
                    while (entityEnumerator.NextEntityIndex(out var i))
                    {
                        var rbIndex = this.FirstBodyIndex + firstEntityIndexInQuery + i;

                        // We support rigid body entities with various different transformation data components.
                        // Here, we are extracting their world space transformation and feed it into the underlying
                        // physics engine for processing in the pipeline (e.g., collision detection, solver, integration).
                        // If the rigid body has a Parent, we obtain their world space transformation from their up-to date
                        // LocalToWorld matrix. Any shear or scale in this matrix is ignored and only the rigid body transformation,
                        // i.e., the position and orientation, is extracted.
                        // If the rigid body has no Parent, we use the position and orientation of its LocalTransform component as world transform
                        // if present. Otherwise, we again extract the transformation from the LocalToWorld matrix.
                        if (hasChunkParentType || !hasChunkLocalTransformType)
                        {
                            if (hasChunkLocalToWorldType)
                            {
                                var localToWorld = chunkLocalToWorlds[i];
                                worldFromBody = Math.DecomposeRigidBodyTransform(localToWorld.Value);
                            }
                        }
                        else
                        {
                            worldFromBody.pos = chunkLocalTransforms[i].Position;
                            worldFromBody.rot = chunkLocalTransforms[i].Rotation;
                        }

                        var scale = 1.0f;
                        if (hasChunkLocalTransformType)
                        {
                            scale = chunkLocalTransforms[i].Scale;
                        }

                        this.RigidBodies[rbIndex] = new RigidBody
                        {
                            WorldFromBody = new RigidTransform(worldFromBody.rot, worldFromBody.pos),
                            Scale = scale,
                            Collider = hasChunkPhysicsColliderType ? chunkColliders[i].Value : default,
                            Entity = chunkEntities[i],
                            CustomTags = hasChunkPhysicsCustomTagsType ? chunkCustomTags[i].Value : (byte)0,
                        };

                        // TODO this actually only needs to be done on a full build
                        keys[rbIndex] = chunkEntities[i];
                        values[rbIndex] = rbIndex;
                    }
                }
            }

            [BurstCompile]
            internal struct RebuildHashMap : IJob
            {
                public NativeHashMap<Entity, int> EntityBodyIndexMap;

                public int Length;

                public void Execute()
                {
                    this.EntityBodyIndexMap.RecalculateBuckets(this.Length);
                }
            }

            [BurstCompile]
            internal struct CreateCreateRigidBodiesFullJob : IJobChunk
            {
                public CreateRigidBodies CreateRigidBodies;

                public void Execute(in ArchetypeChunk chunk, int unfilteredChunkIndex, bool useEnabledMask, in v128 chunkEnabledMask)
                {
                    this.CreateRigidBodies.Execute(chunk, unfilteredChunkIndex, useEnabledMask, chunkEnabledMask);
                }
            }

            [BurstCompile]
            internal struct CreateCreateRigidBodiesPartialJob : IJobChunk
            {
                public CreateRigidBodies CreateRigidBodies;
                public uint LastSystemVersion;

                [NativeDisableParallelForRestriction]
                public NativeReference<int> Changed;

                public void Execute(in ArchetypeChunk chunk, int unfilteredChunkIndex, bool useEnabledMask, in v128 chunkEnabledMask)
                {
                    SafetyChecks.CheckAreEqualAndThrow(false, useEnabledMask);

                    // Order isn't checked as if any order changes this job won't run
                    var didBatchChange = chunk.DidChange(ref this.CreateRigidBodies.LocalToWorldType, this.LastSystemVersion) ||
                        chunk.DidChange(ref this.CreateRigidBodies.LocalTransformType, this.LastSystemVersion) ||
                        chunk.DidChange(ref this.CreateRigidBodies.PhysicsColliderType, this.LastSystemVersion);

                    if (didBatchChange)
                    {
                        this.Changed.Value = 1;
                    }
                    else
                    {
                        didBatchChange |= chunk.DidChange(ref this.CreateRigidBodies.PhysicsCustomTagsType, this.LastSystemVersion);
                    }

                    if (didBatchChange)
                    {
                        // UnityEngine.Debug.Log("Execute");
                        this.CreateRigidBodies.Execute(chunk, unfilteredChunkIndex, useEnabledMask, chunkEnabledMask);
                    }
                    else
                    {
                        // UnityEngine.Debug.Log("Skipped");
                    }
                }
            }

            [BurstCompile]
            internal struct CreateMotions : IJobChunk
            {
                [ReadOnly]
                public ComponentTypeHandle<LocalTransform> LocalTransformType;
                [ReadOnly]
                public ComponentTypeHandle<PhysicsVelocity> PhysicsVelocityType;
                [ReadOnly]
                public ComponentTypeHandle<PhysicsMass> PhysicsMassType;
                [ReadOnly]
                public ComponentTypeHandle<PhysicsMassOverride> PhysicsMassOverrideType;
                [ReadOnly]
                public ComponentTypeHandle<PhysicsDamping> PhysicsDampingType;
                [ReadOnly]
                public ComponentTypeHandle<PhysicsGravityFactor> PhysicsGravityFactorType;
                [ReadOnly]
                public ComponentTypeHandle<Simulate> SimulateType;

                [NativeDisableParallelForRestriction]
                public NativeArray<MotionData> MotionDatas;
                [NativeDisableParallelForRestriction]
                public NativeArray<MotionVelocity> MotionVelocities;
                [ReadOnly]
                public NativeArray<int> ChunkBaseEntityIndices;

                public void Execute(in ArchetypeChunk chunk, int unfilteredChunkIndex, bool useEnabledMask, in v128 chunkEnabledMask)
                {
                    var firstEntityIndexInQuery = this.ChunkBaseEntityIndices[unfilteredChunkIndex];
                    var chunkLocalTransforms = chunk.GetNativeArray(ref this.LocalTransformType);
                    var chunkVelocities = chunk.GetNativeArray(ref this.PhysicsVelocityType);
                    var chunkMasses = chunk.GetNativeArray(ref this.PhysicsMassType);
                    var chunkMassOverrides = chunk.GetNativeArray(ref this.PhysicsMassOverrideType);
                    var chunkDampings = chunk.GetNativeArray(ref this.PhysicsDampingType);
                    var chunkGravityFactors = chunk.GetNativeArray(ref this.PhysicsGravityFactorType);

                    var motionStart = firstEntityIndexInQuery;

                    var hasChunkPhysicsGravityFactorType = chunkGravityFactors.IsCreated;
                    var hasChunkPhysicsDampingType = chunkDampings.IsCreated;
                    var hasChunkPhysicsMassType = chunkMasses.IsCreated;
                    var hasChunkPhysicsMassOverrideType = chunkMassOverrides.IsCreated;
                    var hasChunkLocalTransformType = chunkLocalTransforms.IsCreated;
                    // Note: Transform and AngularExpansionFactor could be calculated from PhysicsCollider.MassProperties
                    // However, to avoid the cost of accessing the collider we assume an infinite mass at the origin of a ~1m^3 box.
                    // For better performance with spheres, or better behavior for larger and/or more irregular colliders
                    // you should add a PhysicsMass component to get the true values
                    var defaultPhysicsMass = new PhysicsMass
                    {
                        Transform = RigidTransform.identity,
                        InverseMass = 0.0f,
                        InverseInertia = float3.zero,
                        AngularExpansionFactor = 1.0f,
                    };

                    var zeroPhysicsVelocity = new PhysicsVelocity
                    {
                        Linear = float3.zero,
                        Angular = float3.zero,
                    };

                    // Note: if a dynamic body has infinite mass then assume no gravity should be applied
                    var defaultGravityFactor = hasChunkPhysicsMassType ? 1.0f : 0.0f;

                    var entityEnumerator1 = new ChunkEntityEnumerator(useEnabledMask, chunkEnabledMask, chunk.Count);
                    while (entityEnumerator1.NextEntityIndex(out var i))
                    {
                        var motionIndex = motionStart + i;
                        // A Body is Kinematic if it has no Mass component, or the Mass component is being overridden.
                        var isKinematic = !hasChunkPhysicsMassType || (hasChunkPhysicsMassOverrideType && chunkMassOverrides[i].IsKinematic != 0) ||
                            !chunk.IsComponentEnabled(ref this.SimulateType, i);

                        var mass = isKinematic ? defaultPhysicsMass : chunkMasses[i];
                        // If the Body is Kinematic its corresponding velocities may be optionally set to zero.
                        var setVelocityToZero = isKinematic && ((hasChunkPhysicsMassOverrideType && chunkMassOverrides[i].SetVelocityToZero != 0) ||
                            !chunk.IsComponentEnabled(ref this.SimulateType, i));

                        var velocity = setVelocityToZero ? zeroPhysicsVelocity : chunkVelocities[i];
                        // If the Body is Kinematic or has an infinite mass gravity should also have no affect on the body's motion.
                        var hasInfiniteMass = isKinematic || mass.HasInfiniteMass;
                        var gravityFactor = hasInfiniteMass ? 0 :
                            hasChunkPhysicsGravityFactorType ? chunkGravityFactors[i].Value : defaultGravityFactor;

                        if (hasChunkLocalTransformType)
                        {
                            mass = mass.ApplyScale(chunkLocalTransforms[i].Scale);
                        }

                        this.MotionVelocities[motionIndex] = new MotionVelocity
                        {
                            LinearVelocity = velocity.Linear,
                            AngularVelocity = velocity.Angular,
                            InverseInertia = mass.InverseInertia,
                            InverseMass = mass.InverseMass,
                            AngularExpansionFactor = mass.AngularExpansionFactor,
                            GravityFactor = gravityFactor,
                        };
                    }

                    // Note: these defaults assume a dynamic body with infinite mass, hence no damping
                    var defaultPhysicsDamping = new PhysicsDamping
                    {
                        Linear = 0.0f,
                        Angular = 0.0f,
                    };

                    // Create motion datas
                    var entityEnumerator2 = new ChunkEntityEnumerator(useEnabledMask, chunkEnabledMask, chunk.Count);
                    while (entityEnumerator2.NextEntityIndex(out var i))
                    {
                        var motionIndex = motionStart + i;
                        // Note that the assignment of the PhysicsMass component is different from the previous loop
                        // as the motion space transform, and not mass & inertia properties are needed here.
                        var mass = hasChunkPhysicsMassType ? chunkMasses[i] : defaultPhysicsMass;
                        var damping = hasChunkPhysicsDampingType ? chunkDampings[i] : defaultPhysicsDamping;
                        // A Body is Kinematic if it has no Mass component, or the Mass component is being overridden.
                        var isKinematic = !hasChunkPhysicsMassType || (hasChunkPhysicsMassOverrideType && chunkMassOverrides[i].IsKinematic != 0) ||
                            !chunk.IsComponentEnabled(ref this.SimulateType, i);
                        // If the Body is Kinematic no resistive damping should be applied to it.

                        var bodyRotationInWorld = quaternion.identity;
                        var bodyPosInWorld = float3.zero;

                        if (hasChunkLocalTransformType)
                        {
                            bodyRotationInWorld = chunkLocalTransforms[i].Rotation;
                            bodyPosInWorld = chunkLocalTransforms[i].Position;

                            mass = mass.ApplyScale(chunkLocalTransforms[i].Scale);
                        }

                        this.MotionDatas[motionIndex] = new MotionData
                        {
                            WorldFromMotion =
                                new RigidTransform(math.mul(bodyRotationInWorld, mass.InertiaOrientation),
                                    math.rotate(bodyRotationInWorld, mass.CenterOfMass) + bodyPosInWorld),
                            BodyFromMotion = new RigidTransform(mass.InertiaOrientation, mass.CenterOfMass),
                            LinearDamping = isKinematic || mass.HasInfiniteMass ? 0.0f : damping.Linear,
                            AngularDamping = isKinematic || mass.HasInfiniteInertia ? 0.0f : damping.Angular,
                        };
                    }
                }
            }

            [BurstCompile]
            internal struct CreateJoints : IJobChunk
            {
                [ReadOnly]
                public ComponentTypeHandle<PhysicsConstrainedBodyPair> ConstrainedBodyPairComponentType;
                [ReadOnly]
                public ComponentTypeHandle<PhysicsJoint> JointComponentType;
                [ReadOnly]
                public EntityTypeHandle EntityType;
                [ReadOnly]
                public int NumDynamicBodies;
                [ReadOnly]
                public NativeHashMap<Entity, int> EntityBodyIndexMap;

                [NativeDisableParallelForRestriction]
                public NativeArray<Joint> Joints;
                [NativeDisableParallelForRestriction]
                public NativeParallelHashMap<Entity, int>.ParallelWriter EntityJointIndexMap;
                [ReadOnly]
                public NativeArray<int> ChunkBaseEntityIndices;

                public int DefaultStaticBodyIndex;

                public void Execute(in ArchetypeChunk chunk, int unfilteredChunkIndex, bool useEnabledMask, in v128 chunkEnabledMask)
                {
                    var firstEntityIndex = this.ChunkBaseEntityIndices[unfilteredChunkIndex];
                    var chunkBodyPair = chunk.GetNativeArray(ref this.ConstrainedBodyPairComponentType);
                    var chunkJoint = chunk.GetNativeArray(ref this.JointComponentType);
                    var chunkEntities = chunk.GetNativeArray(this.EntityType);

                    var entityEnumerator = new ChunkEntityEnumerator(useEnabledMask, chunkEnabledMask, chunk.Count);
                    while (entityEnumerator.NextEntityIndex(out var i))
                    {
                        var bodyPair = chunkBodyPair[i];
                        var entityA = bodyPair.EntityA;
                        var entityB = bodyPair.EntityB;
                        Assert.IsTrue(entityA != entityB);

                        var joint = chunkJoint[i];

                        // TODO find a reasonable way to look up the constraint body indices
                        // - stash body index in a component on the entity? But we don't have random access to Entity data in a job
                        // - make a map from entity to rigid body index? Sounds bad and I don't think there is any NativeArray-based map data structure yet

                        // If one of the entities is null, use the default static entity
                        var pair = new BodyIndexPair
                        {
                            BodyIndexA = entityA == Entity.Null ? this.DefaultStaticBodyIndex : -1,
                            BodyIndexB = entityB == Entity.Null ? this.DefaultStaticBodyIndex : -1,
                        };

                        // Find the body indices
                        pair.BodyIndexA = this.EntityBodyIndexMap.TryGetValue(entityA, out var idxA) ? idxA : -1;
                        pair.BodyIndexB = this.EntityBodyIndexMap.TryGetValue(entityB, out var idxB) ? idxB : -1;

                        var isInvalid = false;
                        // Invalid if we have not found the body indices...
                        isInvalid |= pair.BodyIndexA == -1 || pair.BodyIndexB == -1;
                        // ... or if we are constraining two static bodies
                        // Mark static-static invalid since they are not going to affect simulation in any way.
                        isInvalid |= pair.BodyIndexA >= this.NumDynamicBodies && pair.BodyIndexB >= this.NumDynamicBodies;
                        if (isInvalid)
                        {
                            pair = BodyIndexPair.Invalid;
                        }

                        this.Joints[firstEntityIndex + i] = new Joint
                        {
                            BodyPair = pair,
                            Entity = chunkEntities[i],
                            EnableCollision = (byte)chunkBodyPair[i].EnableCollision,
                            AFromJoint = joint.BodyAFromJoint.AsMTransform(),
                            BFromJoint = joint.BodyBFromJoint.AsMTransform(),
                            Version = joint.Version,
                            Constraints = joint.m_Constraints,
                        };

                        this.EntityJointIndexMap.TryAdd(chunkEntities[i], firstEntityIndex + i);
                    }
                }
            }
        }
#endregion
    }
}
