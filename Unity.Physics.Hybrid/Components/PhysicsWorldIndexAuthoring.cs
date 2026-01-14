using Unity.Entities;
using UnityEngine;

namespace Unity.Physics.Authoring
{
    /// <summary>
    /// Authoring component to associate a rigid body and its joints with the specified physics world.
    /// </summary>
    [Icon(k_IconPath)]
    [AddComponentMenu("Entities/Physics/Physics World Index")]
    [HelpURL(HelpURLs.PhysicsWorldIndexAuthoring)]
    [DisallowMultipleComponent]
    [RequireComponent(typeof(Rigidbody))]
    public class PhysicsWorldIndexAuthoring : MonoBehaviour
    {
        const string k_IconPath = "Packages/com.unity.physics/Unity.Physics.Editor/Editor Default Resources/Icons/d_Rigidbody@64.png";

        /// <summary>
        /// The physics world index.
        ///
        /// The index of the physics world the rigid body and its joints will be associated to. The default physics world has index 0."
        /// </summary>
        public uint WorldIndex { get => m_WorldIndex; set => m_WorldIndex = value; }
        [SerializeField]
        [Tooltip("The index of the physics world the rigid body and its joints will be associated to. The default physics world has index 0.")]
        uint m_WorldIndex = 0;

        void OnEnable()
        {
            // included so tick box appears in Editor
        }
    }

    [TemporaryBakingType]
    struct PhysicsWorldIndexBakingData : IComponentData
    {
        public uint WorldIndex;
    }

    class PhysicsWorldIndexBaker : Baker<PhysicsWorldIndexAuthoring>
    {
        public override void Bake(PhysicsWorldIndexAuthoring authoring)
        {
            var entity = GetEntity(TransformUsageFlags.None);
            AddComponent(entity, new PhysicsWorldIndexBakingData { WorldIndex = authoring.WorldIndex});
        }
    }
}
