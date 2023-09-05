using System;
using Unity.Entities;
using NUnit.Framework;
using Unity.Physics.Authoring;

namespace Unity.Physics.Tests.Authoring
{
    class RigidbodyAndColliderConversionSystemsTransformSystemsIntegrationTests : BaseHierarchyConversionTest
    {
        [TestCaseSource(nameof(k_ExplicitRigidbodyHierarchyTestCases))]
        public void ConversionSystems_WhenChildGOHasExplicitRigidbody_EntityIsInExpectedHierarchyLocation(
            BodyMotionType motionType, EntityQueryDesc expectedQuery
        )
        {
            CreateHierarchy(
                Array.Empty<Type>(),
                Array.Empty<Type>(),
                new[] { typeof(UnityEngine.Rigidbody), typeof(UnityEngine.BoxCollider) }
            );
            Child.GetComponent<UnityEngine.Rigidbody>().isKinematic = motionType != BodyMotionType.Dynamic;
            Child.gameObject.isStatic = motionType == BodyMotionType.Static;

            TransformConversionUtils.ConvertHierarchyAndUpdateTransformSystemsVerifyEntityExists(Root, expectedQuery);
        }
    }
}
