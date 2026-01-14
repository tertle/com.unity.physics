using Unity.Physics.Authoring;
using UnityEditor;
using UnityEngine;

namespace Unity.Physics.Editor
{
    [CustomEditor(typeof(PhysicsStepAuthoring))]
    [CanEditMultipleObjects]
    class PhysicsStepAuthoringEditor : BaseEditor
    {
        static class Content
        {
            public static readonly GUIContent SolverStabilizationLabelUnityPhysics = EditorGUIUtility.TrTextContent("Enable Contact Solver Stabilization Heuristic",
                "Specifies whether the contact solver stabilization heuristic should be applied. Enabling this will result in better overall stability of bodies and piles, " +
                "but may result in behavior artifacts.");
        }

#pragma warning disable 649
        [AutoPopulate] SerializedProperty m_SimulationType;
        [AutoPopulate] SerializedProperty m_Gravity;
        [AutoPopulate] SerializedProperty m_EnableGyroscopicTorque;
        [AutoPopulate] SerializedProperty m_SubstepCount;
        [AutoPopulate] SerializedProperty m_SolverIterationCount;
        [AutoPopulate] SerializedProperty m_DirectSolverSettings;
        [AutoPopulate] SerializedProperty m_EnableSolverStabilizationHeuristic;
        [AutoPopulate] SerializedProperty m_MultiThreaded;
        [AutoPopulate] SerializedProperty m_CollisionTolerance;
        [AutoPopulate] SerializedProperty m_SynchronizeCollisionWorld;
        [AutoPopulate] SerializedProperty m_IncrementalDynamicBroadphase;
        [AutoPopulate] SerializedProperty m_IncrementalStaticBroadphase;
        [AutoPopulate] SerializedProperty m_MaxDynamicDepenetrationVelocity;
        [AutoPopulate] SerializedProperty m_MaxStaticDepenetrationVelocity;
#pragma warning restore 649

        public override void OnInspectorGUI()
        {
            serializedObject.Update();

            EditorGUI.BeginChangeCheck();

            EditorGUILayout.PropertyField(m_SimulationType);

            using (new EditorGUI.DisabledScope(m_SimulationType.intValue == (int)SimulationType.NoPhysics))
            {
                EditorGUILayout.PropertyField(m_Gravity);
                EditorGUILayout.PropertyField(m_EnableGyroscopicTorque);
                EditorGUILayout.PropertyField(m_SubstepCount);
                EditorGUILayout.PropertyField(m_SolverIterationCount);
                EditorGUILayout.PropertyField(m_DirectSolverSettings);
                EditorGUILayout.PropertyField(m_MultiThreaded);
                EditorGUILayout.PropertyField(m_CollisionTolerance);
                EditorGUILayout.PropertyField(m_MaxDynamicDepenetrationVelocity);
                EditorGUILayout.PropertyField(m_MaxStaticDepenetrationVelocity);
                EditorGUILayout.PropertyField(m_SynchronizeCollisionWorld);
                EditorGUILayout.PropertyField(m_IncrementalDynamicBroadphase);
                EditorGUILayout.PropertyField(m_IncrementalStaticBroadphase);
                EditorGUILayout.PropertyField(m_EnableSolverStabilizationHeuristic, Content.SolverStabilizationLabelUnityPhysics);
            }

            if (EditorGUI.EndChangeCheck())
                serializedObject.ApplyModifiedProperties();
        }
    }
}
