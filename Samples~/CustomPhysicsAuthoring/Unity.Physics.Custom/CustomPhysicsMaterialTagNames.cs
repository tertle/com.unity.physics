using System;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Serialization;

namespace Unity.Physics.Authoring
{
    [CreateAssetMenu(menuName = "Unity Physics/Custom Physics Material Tag Names", fileName = "Custom Material Tag Names", order = 506)]
    public sealed partial class CustomPhysicsMaterialTagNames : ScriptableObject, ITagNames
    {
        CustomPhysicsMaterialTagNames() {}

        public IReadOnlyList<string> TagNames => m_TagNames;
        [SerializeField]
        [FormerlySerializedAs("m_FlagNames")]
        string[] m_TagNames = { string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty };

        void OnValidate()
        {
            if (m_TagNames.Length != 8)
                Array.Resize(ref m_TagNames, 8);
        }
    }
}
