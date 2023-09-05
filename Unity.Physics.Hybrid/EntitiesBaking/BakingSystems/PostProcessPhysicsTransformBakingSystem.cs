using Unity.Entities;
using Unity.Mathematics;
using Unity.Transforms;

namespace Unity.Physics.Authoring
{
    /// <summary>
    /// Represents a system that applies and rescales a non-identity scale to a uniform scale
    /// </summary>
    [RequireMatchingQueriesForUpdate]
    [UpdateAfter(typeof(RigidbodyBakingSystem))]
    [WorldSystemFilter(WorldSystemFilterFlags.BakingSystem)]
    public partial class PostProcessPhysicsTransformBakingSystem : SystemBase
    {
        internal void PostProcessTransformComponents(Entity entity, PhysicsPostProcessData physicsPostProcessData)
        {
            var rigidBodyTransform = Math.DecomposeRigidBodyTransform(physicsPostProcessData.LocalToWorldMatrix);

            var manager = EntityManager;

            if (math.lengthsq((float3)physicsPostProcessData.LossyScale - new float3(1f)) > .0001f)
            {
                // Any non-identity scale at authoring time is baked into the physics collision shape/mass data.
                // In this case, the LocalTransform scale field should be set to 1.0 to avoid double-scaling
                // within the physics simulation. We bake the scale into the PostTransformMatrix to make sure the object
                // is rendered correctly.
                var compositeScale = math.mul(
                    math.inverse(new float4x4(rigidBodyTransform)),
                    physicsPostProcessData.LocalToWorldMatrix
                );
                manager.SetComponentData(entity, new PostTransformMatrix { Value = compositeScale });
            }
            var uniformScale = 1.0f;
            manager.SetComponentData(entity,
                LocalTransform.FromPositionRotationScale(rigidBodyTransform.pos, rigidBodyTransform.rot, uniformScale));
        }

        protected override void OnUpdate()
        {
            foreach (var(postProcessData, entity) in SystemAPI.Query<RefRO<PhysicsPostProcessData>>()
                     .WithEntityAccess()
                     .WithOptions(EntityQueryOptions.IncludePrefab | EntityQueryOptions.IncludeDisabledEntities))
            {
                PostProcessTransformComponents(entity, postProcessData.ValueRO);
            }
        }
    }
}
