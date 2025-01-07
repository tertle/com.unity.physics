using Unity.Burst;
using Unity.Burst.Intrinsics;
using Unity.Collections;
using Unity.Entities;
using Unity.Jobs;
using Unity.Mathematics;
using Unity.Transforms;

namespace Unity.Physics.Systems
{
    /// <summary>   Utilities for exporting physics world data to ECS components. </summary>
    public static class PhysicsWorldExporter
    {
        /// <summary>   Stores the ECS component handles needed to export <see cref="PhysicsWorld"/> to ECS components. </summary>
        public struct ExportPhysicsWorldTypeHandles
        {
            /// <summary>   Constructor. </summary>
            ///
            /// <param name="systemState">  [in,out] State of the system. </param>
            public ExportPhysicsWorldTypeHandles(ref SystemState systemState)
            {
                LocalTransformType = systemState.GetComponentTypeHandle<LocalTransform>(false);

                PhysicsVelocityType = systemState.GetComponentTypeHandle<PhysicsVelocity>(false);
                SimulateType = systemState.GetComponentTypeHandle<Simulate>(true);
            }

            /// <summary>   Updates the component handles. Call this in OnUpdate() methods of the system you want to export physics world. </summary>
            ///
            /// <param name="systemState">  [in,out] State of the system. </param>
            public void Update(ref SystemState systemState)
            {
                LocalTransformType.Update(ref systemState);

                PhysicsVelocityType.Update(ref systemState);
                SimulateType.Update(ref systemState);
            }

            internal ComponentTypeHandle<LocalTransform> LocalTransformType;

            internal ComponentTypeHandle<PhysicsVelocity> PhysicsVelocityType;
            internal ComponentTypeHandle<Simulate> SimulateType;
        }

        /// <summary>
        /// Schedules a job that copies positions and velocities of all dynamic bodies from specified
        /// PhysicsWorld to ECS components of entities returned by specified query. No need to call <see cref="ExportPhysicsWorldTypeHandles.Update(ref SystemState)"/>
        /// prior to calling this method.
        /// </summary>
        ///
        /// <param name="systemState">          [in,out] State of the system. </param>
        /// <param name="componentTypeHandles"> [in,out] The component type handles. </param>
        /// <param name="world">                The world. </param>
        /// <param name="inputDep">             The input dependency. </param>
        /// <param name="dynamicEntities">      The dynamic entities. </param>
        ///
        /// <returns>   A JobHandle. </returns>
        public static JobHandle SchedulePhysicsWorldExport(
            ref SystemState systemState,
            ref ExportPhysicsWorldTypeHandles componentTypeHandles,
            in PhysicsWorld world,
            in JobHandle inputDep,
            EntityQuery dynamicEntities)
        {
            if (world.NumDynamicBodies > 0)
            {
                var chunkBaseEntityIndices =
                    dynamicEntities.CalculateBaseEntityIndexArrayAsync(systemState.WorldUpdateAllocator, inputDep,
                        out var baseIndexJob);

                componentTypeHandles.Update(ref systemState);
                return new ExportDynamicBodiesJob
                {
                    MotionVelocities = world.MotionVelocities,
                    MotionDatas = world.MotionDatas,

                    LocalTransformType = componentTypeHandles.LocalTransformType,

                    VelocityType = componentTypeHandles.PhysicsVelocityType,
                    SimulateType = componentTypeHandles.SimulateType,
                    ChunkBaseEntityIndices = chunkBaseEntityIndices,
                }.ScheduleParallel(dynamicEntities, baseIndexJob);
            }
            return inputDep;
        }

        #region Jobs

        [BurstCompile]
        internal struct ExportDynamicBodiesJob : IJobChunk
        {
            [ReadOnly] public NativeArray<MotionVelocity> MotionVelocities;
            [ReadOnly] public NativeArray<MotionData> MotionDatas;
            [ReadOnly] public NativeArray<int> ChunkBaseEntityIndices;

            public ComponentTypeHandle<LocalTransform> LocalTransformType;

            public ComponentTypeHandle<PhysicsVelocity> VelocityType;
            [ReadOnly] public ComponentTypeHandle<Simulate> SimulateType;

            public void Execute(in ArchetypeChunk chunk, int unfilteredChunkIndex, bool useEnabledMask, in v128 chunkEnabledMask)
            {
                int entityStartIndex = ChunkBaseEntityIndices[unfilteredChunkIndex];

                var chunkLocalTransforms = chunk.GetNativeArray(ref LocalTransformType);

                var chunkVelocities = chunk.GetNativeArray(ref VelocityType);

                var entityEnumerator = new ChunkEntityEnumerator(useEnabledMask, chunkEnabledMask, chunk.Count);
                var isEntitySimulated = chunk.GetEnabledMask(ref SimulateType);
                while (entityEnumerator.NextEntityIndex(out var i))
                {
                    //If an entity is not simulated (so it is marked as kinematic and has also zero velocity)
                    //we should not export the velocity, position and orientation.
                    //Technically would be better to add the Simulate component to the query. But this will break other jobs
                    //that can't handle the enable mask. So we are doing a check here on an entity by entity basis.
                    if (!isEntitySimulated[i])
                        continue;
                    int motionIndex = entityStartIndex + i;
                    MotionData md = MotionDatas[motionIndex];
                    RigidTransform worldFromBody = math.mul(md.WorldFromMotion, math.inverse(md.BodyFromMotion));

                    var localToWorldTransform = chunkLocalTransforms[i];
                    localToWorldTransform.Position = worldFromBody.pos;
                    localToWorldTransform.Rotation = worldFromBody.rot;
                    chunkLocalTransforms[i] = localToWorldTransform;

                    chunkVelocities[i] = new PhysicsVelocity
                    {
                        Linear = MotionVelocities[motionIndex].LinearVelocity,
                        Angular = MotionVelocities[motionIndex].AngularVelocity
                    };
                }
            }
        }
        #endregion
    }
}
