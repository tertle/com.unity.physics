using System;
using System.Collections.Generic;
using UnityEngine;

namespace Unity.Physics.Authoring
{
    [CreateAssetMenu(menuName = "Unity Physics/Physics Category Names", fileName = "Physics Category Names", order = 507)]
    public sealed class PhysicsCategoryNames : ScriptableObject, ITagNames
    {
        PhysicsCategoryNames() {}

        IReadOnlyList<string> ITagNames.TagNames => CategoryNames;

        public IReadOnlyList<string> CategoryNames => m_CategoryNames;
        [SerializeField]
        string[] m_CategoryNames =
        {
            string.Empty, string.Empty, string.Empty, string.Empty,
            string.Empty, string.Empty, string.Empty, string.Empty,
            string.Empty, string.Empty, string.Empty, string.Empty,
            string.Empty, string.Empty, string.Empty, string.Empty,
            string.Empty, string.Empty, string.Empty, string.Empty,
            string.Empty, string.Empty, string.Empty, string.Empty,
            string.Empty, string.Empty, string.Empty, string.Empty,
            string.Empty, string.Empty, string.Empty, string.Empty
        };

        void OnValidate()
        {
            if (m_CategoryNames.Length != 32)
                Array.Resize(ref m_CategoryNames, 32);
        }
    }
}
