#if UNITY_EDITOR
using System;
using System.Collections.Generic;
using UnityEditor;
using UnityEditor.Build;

namespace Unity.Physics.Authoring
{
    [InitializeOnLoad]
    class EditorInitialization
    {
        static readonly string k_CustomDefine = "UNITY_PHYSICS_CUSTOM";

        static EditorInitialization()
        {
            var fromBuildTargetGroup = NamedBuildTarget.FromBuildTargetGroup(EditorUserBuildSettings.selectedBuildTargetGroup);
            var definesStr = PlayerSettings.GetScriptingDefineSymbols(fromBuildTargetGroup);
            var defines = new List<string>(definesStr.Split(';'));

            bool found = false;
            foreach (var define in defines)
            {
                if (define.Equals(k_CustomDefine))
                {
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                defines.Add(k_CustomDefine);
                PlayerSettings.SetScriptingDefineSymbols(fromBuildTargetGroup, string.Join(";", defines.ToArray()));
            }
        }
    }
}
#endif
