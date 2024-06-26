using System.Collections.Generic;
using Unity.Entities;
using Unity.Mathematics;
using UnityEngine;

namespace Unity.Physics.Authoring
{
    internal class DisplayGizmoColliderEdges
    {
        unsafe class DrawWithGizmos : MonoBehaviour
        {
            static void GetEdge(ref ConvexHull hullIn, ConvexHull.Face faceIn, int edgeIndex, out float3 from, out float3 to)
            {
                byte fromIndex = hullIn.FaceVertexIndices[faceIn.FirstIndex + edgeIndex];
                byte toIndex = hullIn.FaceVertexIndices[faceIn.FirstIndex + (edgeIndex + 1) % faceIn.NumVertices];
                from = hullIn.Vertices[fromIndex];
                to = hullIn.Vertices[toIndex];
            }

            static void DrawColliderEdges(ConvexCollider* collider, RigidTransform worldFromConvex, bool drawVertices = false)
            {
                Matrix4x4 originalMatrix = Gizmos.matrix;
                Color originalColor = Gizmos.color;
                Gizmos.matrix = math.float4x4(worldFromConvex);

                ref ConvexHull hull = ref collider->ConvexHull;

                if (hull.FaceLinks.Length > 0)
                {
                    Gizmos.color = new Color(0.0f, 1.0f, 0.0f);
                    foreach (ConvexHull.Face face in hull.Faces)
                    {
                        for (int edgeIndex = 0; edgeIndex < face.NumVertices; edgeIndex++)
                        {
                            GetEdge(ref hull, face, edgeIndex, out float3 from, out float3 to);
                            Gizmos.DrawLine(from, to);
                        }
                    }
                }
                else
                {
                    Gizmos.color = new Color(0.0f, 1.0f, 0.0f);
                    switch (collider->Type)
                    {
                        case ColliderType.Capsule:
                            Gizmos.DrawLine(hull.Vertices[0], hull.Vertices[1]);
                            break;
                        case ColliderType.Cylinder:
                            for (int f = 0; f < hull.Faces.Length; f++)
                            {
                                var face = hull.Faces[f];
                                for (int v = 0; v < face.NumVertices - 1; v++)
                                {
                                    byte i = hull.FaceVertexIndices[face.FirstIndex + v];
                                    byte j = hull.FaceVertexIndices[face.FirstIndex + v + 1];
                                    Gizmos.DrawLine(hull.Vertices[i], hull.Vertices[j]);
                                }
                                // Draw final line between first and last vertices
                                {
                                    byte i = hull.FaceVertexIndices[face.FirstIndex + face.NumVertices - 1];
                                    byte j = hull.FaceVertexIndices[face.FirstIndex];
                                    Gizmos.DrawLine(hull.Vertices[i], hull.Vertices[j]);
                                }
                            }
                            break;
                        case ColliderType.Sphere:
                            // No edges on sphere but nice to see center
                            float3 offset = new float3(0.01f);
                            for (int i = 0; i < 3; i++)
                            {
                                offset[i] = -offset[i];
                                Gizmos.DrawLine(hull.Vertices[0] - offset, hull.Vertices[0] + offset);
                            }
                            break;
                    }
                }

                if (drawVertices && hull.VertexEdges.Length > 0)
                {
                    Gizmos.color = new Color(1.0f, 0.0f, 0.0f);
                    foreach (ConvexHull.Edge vertexEdge in hull.VertexEdges)
                    {
                        ConvexHull.Face face = hull.Faces[vertexEdge.FaceIndex];
                        GetEdge(ref hull, face, vertexEdge.EdgeIndex, out float3 from, out float3 to);
                        float3 direction = (to - from) * 0.25f;

                        Gizmos.DrawSphere(from, 0.01f);
                        Gizmos.DrawRay(from, direction);
                    }
                }

                Gizmos.color = originalColor;
                Gizmos.matrix = originalMatrix;
            }

            private struct Edge
            {
                internal Vector3 A;
                internal Vector3 B;
            }

            static void DrawColliderEdges(MeshCollider* meshCollider, RigidTransform worldFromCollider)
            {
                Matrix4x4 originalMatrix = Gizmos.matrix;
                Color originalColor = Gizmos.color;
                Gizmos.matrix = math.float4x4(worldFromCollider);

                ref Mesh mesh = ref meshCollider->Mesh;

                var triangleEdges = new List<Edge>();
                var trianglePairEdges = new List<Edge>();
                var quadEdges = new List<Edge>();

                for (int sectionIndex = 0; sectionIndex < mesh.Sections.Length; sectionIndex++)
                {
                    ref Mesh.Section section = ref mesh.Sections[sectionIndex];
                    for (int primitiveIndex = 0; primitiveIndex < section.PrimitiveVertexIndices.Length; primitiveIndex++)
                    {
                        Mesh.PrimitiveVertexIndices vertexIndices = section.PrimitiveVertexIndices[primitiveIndex];
                        Mesh.PrimitiveFlags flags = section.PrimitiveFlags[primitiveIndex];
                        bool isTrianglePair = flags.HasFlag(Mesh.PrimitiveFlags.IsTrianglePair);
                        bool isQuad = flags.HasFlag(Mesh.PrimitiveFlags.IsQuad);

                        float3x4 v = new float3x4(
                            section.Vertices[vertexIndices.A],
                            section.Vertices[vertexIndices.B],
                            section.Vertices[vertexIndices.C],
                            section.Vertices[vertexIndices.D]);

                        if (isQuad)
                        {
                            quadEdges.Add(new Edge { A = v[0], B = v[1] });
                            quadEdges.Add(new Edge { A = v[1], B = v[2] });
                            quadEdges.Add(new Edge { A = v[2], B = v[3] });
                            quadEdges.Add(new Edge { A = v[3], B = v[0] });
                        }
                        else if (isTrianglePair)
                        {
                            trianglePairEdges.Add(new Edge { A = v[0], B = v[1] });
                            trianglePairEdges.Add(new Edge { A = v[1], B = v[2] });
                            trianglePairEdges.Add(new Edge { A = v[2], B = v[3] });
                            trianglePairEdges.Add(new Edge { A = v[3], B = v[0] });
                            trianglePairEdges.Add(new Edge { A = v[0], B = v[2] });
                        }
                        else
                        {
                            triangleEdges.Add(new Edge { A = v[0], B = v[1] });
                            triangleEdges.Add(new Edge { A = v[1], B = v[2] });
                            triangleEdges.Add(new Edge { A = v[2], B = v[0] });
                        }
                    }
                }

                Gizmos.color = new Color(0.0f, 1.0f, 0.0f);

                foreach (Edge edge in triangleEdges)
                {
                    Gizmos.DrawLine(edge.A, edge.B);
                }

                foreach (Edge edge in trianglePairEdges)
                {
                    Gizmos.DrawLine(edge.A, edge.B);
                }

                foreach (Edge edge in quadEdges)
                {
                    Gizmos.DrawLine(edge.A, edge.B);
                }

                Gizmos.color = originalColor;
                Gizmos.matrix = originalMatrix;
            }

            static void DrawColliderEdges(CompoundCollider* compoundCollider, RigidTransform worldFromCompound, bool drawVertices = false)
            {
                for (int i = 0; i < compoundCollider->NumChildren; i++)
                {
                    ref CompoundCollider.Child child = ref compoundCollider->Children[i];
                    var childCollider = child.Collider;
                    var worldFromChild = math.mul(worldFromCompound, child.CompoundFromChild);
                    DrawColliderEdges(childCollider, worldFromChild);
                }
            }

            static void DrawColliderEdges(Collider* collider, RigidTransform worldFromCollider, bool drawVertices = false)
            {
                switch (collider->CollisionType)
                {
                    case CollisionType.Convex:
                        DrawColliderEdges((ConvexCollider*)collider, worldFromCollider, drawVertices);
                        break;
                    case CollisionType.Composite:
                        switch (collider->Type)
                        {
                            case ColliderType.Compound:
                                DrawColliderEdges((CompoundCollider*)collider, worldFromCollider, drawVertices);
                                break;
                            case ColliderType.Mesh:
                                DrawColliderEdges((MeshCollider*)collider, worldFromCollider);
                                break;
                        }
                        break;
                }
            }

            static void DrawColliderEdges(BlobAssetReference<Collider> collider, RigidTransform worldFromCollider, bool drawVertices)
            {
                // Note: this is an internal utility function used for drawing collider edges through blobs assets.
                // It can be used for drawing colliders in MonoBehavior.OnDrawGizmos() functions.
                // Function is accessed in samples using System.Type.GetMethod().
                DrawColliderEdges((Collider*)collider.GetUnsafePtr(), worldFromCollider, drawVertices);
            }
        }
    }
}
