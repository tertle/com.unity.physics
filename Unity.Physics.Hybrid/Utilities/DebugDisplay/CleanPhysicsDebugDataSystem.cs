using Unity.Entities;
using Unity.Physics.Systems;

namespace Unity.Physics.Authoring
{
#if UNITY_EDITOR || ENABLE_UNITY_PHYSICS_RUNTIME_DEBUG_DISPLAY

    /// <summary>
    /// A system which cleans physics debug display data from the previous frame while in play mode.
    /// When using multiple physics worlds, in order for the debug display to work properly, you need to disable
    /// the update of this system in any <see cref="PhysicsSystemGroup">physics system group</see> following the first one.
    /// </summary>
    [WorldSystemFilter(WorldSystemFilterFlags.Default)]
    [CreateAfter(typeof(PhysicsDebugDisplaySystem_Default))]
    [UpdateInGroup(typeof(PhysicsInitializeGroup), OrderFirst = true)]
    public partial class CleanPhysicsDebugDataSystem_Default : SystemBase
    {
        PhysicsDebugDisplaySystem m_DebugDisplaySystem;

        protected override void OnCreate()
        {
            RequireForUpdate<PhysicsDebugDisplayData>();
            m_DebugDisplaySystem = World.GetExistingSystemManaged<PhysicsDebugDisplaySystem_Default>();
        }

        protected override void OnUpdate()
        {
            m_DebugDisplaySystem?.Clear();
        }
    }

    /// <summary>
    /// A system which cleans physics debug display data from the previous frame while in edit mode.
    /// </summary>
    [WorldSystemFilter(WorldSystemFilterFlags.Editor)]
    [CreateAfter(typeof(PhysicsDebugDisplaySystem_Editor))]
    [UpdateInGroup(typeof(PhysicsDebugDisplayGroup_Editor), OrderFirst = true)]
    public partial class CleanPhysicsDebugDataSystem_Editor : SystemBase
    {
        PhysicsDebugDisplaySystem m_DebugDisplaySystem;

        protected override void OnCreate()
        {
            RequireForUpdate<PhysicsDebugDisplayData>();
            m_DebugDisplaySystem = World.GetExistingSystemManaged<PhysicsDebugDisplaySystem_Editor>();
        }

        protected override void OnUpdate()
        {
            m_DebugDisplaySystem?.Clear();
        }
    }
#endif
}
