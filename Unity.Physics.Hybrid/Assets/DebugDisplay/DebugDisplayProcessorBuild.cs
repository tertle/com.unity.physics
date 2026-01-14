#if UNITY_EDITOR && ENABLE_UNITY_PHYSICS_RUNTIME_DEBUG_DISPLAY
using UnityEditor;
using UnityEngine;
using UnityEditor.Build.Reporting;
using UnityEditor.Build;
using UnityEditor.Callbacks;
using System.IO;
using System;

namespace Unity.DebugDisplay
{
    class DebugDisplayProcessorBuild : IPreprocessBuildWithReport
    {
        public int callbackOrder { get { return 0; } }
        internal static string ResourcesPath => Path.Combine(Renderer.debugDirName, "Resources");

        public void OnPreprocessBuild(BuildReport report)
        {
            try
            {
                if (!Directory.Exists(ResourcesPath))
                {
                    Directory.CreateDirectory(ResourcesPath);
                }

                // Get all .mat and .shader files from debugDirName
                string[] materialFiles = Directory.GetFiles(Renderer.debugDirName, "*.mat", SearchOption.TopDirectoryOnly);
                string[] shaderFiles = Directory.GetFiles(Renderer.debugDirName, "*.shader", SearchOption.TopDirectoryOnly);

                // Combine both arrays
                string[] allFiles = new string[materialFiles.Length + shaderFiles.Length];
                materialFiles.CopyTo(allFiles, 0);
                shaderFiles.CopyTo(allFiles, materialFiles.Length);

                foreach (string file in allFiles)
                {
                    string destinationFile = Path.Combine(ResourcesPath, Path.GetFileName(file));
                    File.Copy(file, destinationFile, true);
                }
            }
            catch (Exception ex)
            {
                Debug.LogError($"<color=red>Build Preprocess Failed</color>: {ex.Message}");
            }
        }

        [PostProcessBuild(1)]
        private static void OnPostprocessBuild(BuildTarget target, string pathToBuiltProject)
        {
            var metaFile = Path.Combine(Renderer.debugDirName, "Resources.meta");
            if (File.Exists(metaFile))
            {
                File.Delete(metaFile);
            }

            if (Directory.Exists(ResourcesPath))
            {
                Directory.Delete(ResourcesPath, true);
            }
        }
    }
}
#endif
