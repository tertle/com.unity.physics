using Unity.Entities;
using Unity.Mathematics;
using Unity.Transforms;

namespace Unity.Physics.Authoring
{
    /// <summary>
    /// Post process baking system which extracts any non-identity scale and skew present on physics entities and bakes
    /// it into the <see cref="PostTransformMatrix">. This is necessary since the transformation of physics entities (e.g., colliders)
    /// is represented by a <see cref="LocalTransform"> component only, which does not support any non-uniform scale or skew.
    /// Consequently, the <see cref="PostTransformMatrix"> is applied to the latest transformation matrix of the entity
    /// after every physics step in order to render the entity correctly.
    /// </summary>
    [RequireMatchingQueriesForUpdate]
    [UpdateAfter(typeof(RigidbodyBakingSystem))]
    [WorldSystemFilter(WorldSystemFilterFlags.BakingSystem)]
    public partial struct PostProcessPhysicsTransformBakingSystem : ISystem
    {
        internal void PostProcessTransformComponents(Entity entity, PhysicsPostProcessData physicsPostProcessData, ref SystemState state)
        {
            var rigidBodyTransform = Math.DecomposeRigidBodyTransform(physicsPostProcessData.LocalToWorldMatrix);

            var manager = state.EntityManager;

            if (HasNonUniformScale((float3)physicsPostProcessData.LossyScale, out var uniformScale))
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
            manager.SetComponentData(entity,
                LocalTransform.FromPositionRotationScale(rigidBodyTransform.pos, rigidBodyTransform.rot, uniformScale));
        }

        public void OnUpdate(ref SystemState state)
        {
            foreach (var(postProcessData, entity) in SystemAPI.Query<RefRO<PhysicsPostProcessData>>()
                     .WithEntityAccess()
                     .WithOptions(EntityQueryOptions.IncludePrefab | EntityQueryOptions.IncludeDisabledEntities))
            {
                PostProcessTransformComponents(entity, postProcessData.ValueRO, ref state);
            }
        }

        bool HasNonUniformScale(float3 lossyScale, out float uniformScale)
        {
            if (math.abs(lossyScale.x - lossyScale.y) > .0001f || math.abs(lossyScale.x - lossyScale.z) > .0001f)
            {
                uniformScale = 1;
                return true;
            }

            uniformScale = lossyScale.x;
            return false;
        }
    }
}
