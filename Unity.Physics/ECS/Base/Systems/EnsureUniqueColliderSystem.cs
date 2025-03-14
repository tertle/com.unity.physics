using Unity.Burst;
using Unity.Entities;
using Unity.Physics.Extensions;

namespace Unity.Physics.Systems
{
    /// <summary>
    /// This system ensures that colliders which are flagged to be made unique via the EnsureUniqueColliderBlobTag
    /// are made unique.
    /// Specifically, it ensures that the forced unique colliders in newly instantiated prefabs are made unique.
    /// </summary>
    [RequireMatchingQueriesForUpdate]
    [UpdateInGroup(typeof(BeforePhysicsSystemGroup), OrderFirst = true)]
    internal partial struct EnsureUniqueColliderSystem : ISystem
    {
        [BurstCompile]
        private partial struct MakeUniqueJob : IJobEntity
        {
            public EntityCommandBuffer.ParallelWriter ECB;

            private void Execute(in Entity entity, in EnsureUniqueColliderBlobTag tag, ref PhysicsCollider collider, [ChunkIndexInQuery] int chunkIndex)
            {
                // If the collider is not unique but should be, we need to ensure it is
                if (!collider.IsUnique)
                {
                    collider.MakeUnique(entity, this.ECB, chunkIndex);
                    this.ECB.RemoveComponent<EnsureUniqueColliderBlobTag>(chunkIndex, entity);
                }
            }
        }

        [BurstCompile]
        public void OnCreate(ref SystemState state)
        {
            state.RequireForUpdate<EndFixedStepSimulationEntityCommandBufferSystem.Singleton>();
            state.RequireForUpdate<EnsureUniqueColliderBlobTag>();
        }

        [BurstCompile]
        public void OnUpdate(ref SystemState state)
        {
            var ecb = SystemAPI.GetSingleton<EndFixedStepSimulationEntityCommandBufferSystem.Singleton>().CreateCommandBuffer(state.WorldUnmanaged);

            // Run on all entities with colliders which are required to be unique and ensure that they are.
            state.Dependency = new MakeUniqueJob
            {
                ECB = ecb.AsParallelWriter(),
            }.ScheduleParallel(state.Dependency);
        }
    }
}
