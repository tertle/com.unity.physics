#if UNITY_EDITOR || ENABLE_UNITY_PHYSICS_RUNTIME_DEBUG_DISPLAY

using System;
using Unity.Collections;
using Unity.Mathematics;

namespace Unity.DebugDisplay
{
    internal struct Arrows : IDisposable
    {
        private Lines m_Lines;
        internal unsafe Arrows(int count, DrawData drawData)
        {
            m_Lines = new Lines(count * 5, drawData);
        }

        internal void Draw(float3 x, float3 v, Unity.DebugDisplay.ColorIndex color)
        {
            var X0 = x;
            var X1 = x + v;

            m_Lines.Draw(X0, X1, color);

            float3 dir;
            float length = Physics.Math.NormalizeWithLength(v, out dir);
            float3 perp, perp2;
            Physics.Math.CalculatePerpendicularNormalized(dir, out perp, out perp2);
            float3 scale = length * 0.2f;

            m_Lines.Draw(X1, X1 + (perp - dir) * scale, color);
            m_Lines.Draw(X1, X1 - (perp + dir) * scale, color);
            m_Lines.Draw(X1, X1 + (perp2 - dir) * scale, color);
            m_Lines.Draw(X1, X1 - (perp2 + dir) * scale, color);
        }

        public void Dispose()
        {
            m_Lines.Dispose();
        }
    }

    internal struct Planes : IDisposable
    {
        private Lines m_Lines;

        internal unsafe Planes(int count, DrawData drawData)
        {
            m_Lines = new Lines(count * 9, drawData);
        }

        internal void Draw(float3 x, float3 v, Unity.DebugDisplay.ColorIndex color)
        {
            var X0 = x;
            var X1 = x + v;

            m_Lines.Draw(X0, X1, color);

            float3 dir;
            float length = Physics.Math.NormalizeWithLength(v, out dir);
            float3 perp, perp2;
            Physics.Math.CalculatePerpendicularNormalized(dir, out perp, out perp2);
            float3 scale = length * 0.2f;

            m_Lines.Draw(X1, X1 + (perp - dir) * scale, color);
            m_Lines.Draw(X1, X1 - (perp + dir) * scale, color);
            m_Lines.Draw(X1, X1 + (perp2 - dir) * scale, color);
            m_Lines.Draw(X1, X1 - (perp2 + dir) * scale, color);

            perp *= length;
            perp2 *= length;

            m_Lines.Draw(X0 + perp + perp2, X0 + perp - perp2, color);
            m_Lines.Draw(X0 + perp - perp2, X0 - perp - perp2, color);
            m_Lines.Draw(X0 - perp - perp2, X0 - perp + perp2, color);
            m_Lines.Draw(X0 - perp + perp2, X0 + perp + perp2, color);
        }

        public void Dispose()
        {
            m_Lines.Dispose();
        }
    }

    internal struct Arcs : IDisposable
    {
        private Lines m_Lines;
        const int res = 16;

        internal unsafe Arcs(int count, DrawData drawData)
        {
            m_Lines = new Lines(count * (2 + res), drawData);
        }

        internal void Draw(float3 center, float3 normal, float3 arm, float angle, Unity.DebugDisplay.ColorIndex color)
        {
            quaternion q = quaternion.AxisAngle(normal, angle / res);
            float3 currentArm = arm;
            m_Lines.Draw(center, center + currentArm, color);
            for (int i = 0; i < res; i++)
            {
                float3 nextArm = math.mul(q, currentArm);
                m_Lines.Draw(center + currentArm, center + nextArm, color);
                currentArm = nextArm;
            }
            m_Lines.Draw(center, center + currentArm, color);
        }

        public void Dispose()
        {
            m_Lines.Dispose();
        }
    }

    internal struct Boxes : IDisposable
    {
        private Lines m_Lines;

        internal unsafe Boxes(int count, DrawData drawData)
        {
            m_Lines = new Lines(count * 12, drawData);
        }

        internal void Draw(float3 Size, float3 Center, quaternion Orientation, Unity.DebugDisplay.ColorIndex color)
        {
            float3x3 mat = math.float3x3(Orientation);
            float3 x = mat.c0 * Size.x * 0.5f;
            float3 y = mat.c1 * Size.y * 0.5f;
            float3 z = mat.c2 * Size.z * 0.5f;
            float3 c0 = Center - x - y - z;
            float3 c1 = Center - x - y + z;
            float3 c2 = Center - x + y - z;
            float3 c3 = Center - x + y + z;
            float3 c4 = Center + x - y - z;
            float3 c5 = Center + x - y + z;
            float3 c6 = Center + x + y - z;
            float3 c7 = Center + x + y + z;

            m_Lines.Draw(c0, c1, color); // ring 0
            m_Lines.Draw(c1, c3, color);
            m_Lines.Draw(c3, c2, color);
            m_Lines.Draw(c2, c0, color);

            m_Lines.Draw(c4, c5, color); // ring 1
            m_Lines.Draw(c5, c7, color);
            m_Lines.Draw(c7, c6, color);
            m_Lines.Draw(c6, c4, color);

            m_Lines.Draw(c0, c4, color); // between rings
            m_Lines.Draw(c1, c5, color);
            m_Lines.Draw(c2, c6, color);
            m_Lines.Draw(c3, c7, color);
        }

        public void Dispose()
        {
            m_Lines.Dispose();
        }
    }

    internal struct Cones : IDisposable
    {
        private Lines m_Lines;
        const int res = 16;

        internal unsafe Cones(int count, DrawData drawData)
        {
            m_Lines = new Lines(count * res * 2, drawData);
        }

        internal void Draw(float3 point, float3 axis, float angle, Unity.DebugDisplay.ColorIndex color)
        {
            float3 dir;
            float scale = Physics.Math.NormalizeWithLength(axis, out dir);
            float3 arm;
            {
                float3 perp1, perp2;
                Physics.Math.CalculatePerpendicularNormalized(dir, out perp1, out perp2);
                arm = math.mul(quaternion.AxisAngle(perp1, angle), dir) * scale;
            }
            quaternion q = quaternion.AxisAngle(dir, 2.0f * (float)math.PI / res);

            for (int i = 0; i < res; i++)
            {
                float3 nextArm = math.mul(q, arm);
                m_Lines.Draw(point, point + arm, color);
                m_Lines.Draw(point + arm, point + nextArm, color);
                arm = nextArm;
            }
        }

        public void Dispose()
        {
            m_Lines.Dispose();
        }
    }

    internal struct Lines : IDisposable
    {
        Unit m_Unit;
        DrawData m_DrawData;

        internal Lines(int count, DrawData drawData)
        {
            m_Unit = drawData.m_LineBuffer.AllocateAtomic(count);
            m_DrawData = drawData;
        }

        internal void Draw(float3 begin, float3 end, ColorIndex color)
        {
            if (m_Unit.m_Next < m_Unit.m_End)
            {
                m_DrawData.m_LineBuffer.SetLine(begin, end, color, m_Unit.m_Next++);
            }
        }

        public void Dispose()
        {
            while (m_Unit.m_Next < m_Unit.m_End)
                m_DrawData.m_LineBuffer.ClearLine(m_Unit.m_Next++);
        }
    }

    //------
    struct Triangles : IDisposable
    {
        Unit m_Unit;
        DrawData m_DrawData;

        internal Triangles(int count, DrawData drawData)
        {
            m_Unit = drawData.m_TriangleBuffer.AllocateAtomic(count);
            m_DrawData = drawData;
        }

        internal void Draw(float3 vertex0, float3 vertex1, float3 vertex2, float3 normal, Unity.DebugDisplay.ColorIndex color)
        {
            if (m_Unit.m_Next < m_Unit.m_End)
            {
                m_DrawData.m_TriangleBuffer.SetTriangle(vertex0, vertex1, vertex2, normal, color,
                    m_Unit.m_Next++);
            }
        }

        public void Dispose()
        {
            while (m_Unit.m_Next < m_Unit.m_End)
                m_DrawData.m_TriangleBuffer.ClearTriangle(m_Unit.m_Next++);
        }
    }

    //--------
    internal struct DrawNumbers : IDisposable
    {
        Unit m_Unit;
        DrawData m_DrawData;

        const int k_MaxSegments = 7;
        static unsafe void GetDigitSegments(in int digit, int* segments, out int segmentCount)
        {
            var s = segments;
            switch (digit)
            {
                case 0:
                {
                    s[0] = 0; s[1] = 1; s[2] = 2; s[3] = 4; s[4] = 5; s[5] = 6;
                    segmentCount = 6;
                    break;
                }
                case 1:
                {
                    s[0] = 2; s[1] = 5;
                    segmentCount = 2;
                    break;
                }
                case 2:
                {
                    s[0] = 0; s[1] = 2; s[2] = 3; s[3] = 4; s[4] = 6;
                    segmentCount = 5;
                    break;
                }
                case 3:
                {
                    s[0] = 0; s[1] = 2; s[2] = 3; s[3] = 5; s[4] = 6;
                    segmentCount = 5;
                    break;
                }
                case 4:
                {
                    s[0] = 1; s[1] = 2; s[2] = 3; s[3] = 5;
                    segmentCount = 4;
                    break;
                }
                case 5:
                {
                    s[0] = 0; s[1] = 1; s[2] = 3; s[3] = 5; s[4] = 6;
                    segmentCount = 5;
                    break;
                }
                case 6:
                {
                    s[0] = 0; s[1] = 1; s[2] = 3; s[3] = 4; s[4] = 5; s[5] = 6;
                    segmentCount = 6;
                    break;
                }
                case 7:
                {
                    s[0] = 0; s[1] = 2; s[2] = 5;
                    segmentCount = 3;
                    break;
                }
                case 8:
                {
                    s[0] = 0; s[1] = 1; s[2] = 2; s[3] = 3; s[4] = 4; s[5] = 5; s[6] = 6;
                    segmentCount = 7;
                    break;
                }
                case 9:
                {
                    s[0] = 0; s[1] = 1; s[2] = 2; s[3] = 3; s[4] = 5; s[5] = 6;
                    segmentCount = 6;
                    break;
                }
                default:
                {
                    segmentCount = 0;
                    break;
                }
            }
        }

        static readonly float[] s_SegmentPoints =
        {
            0, 1, 0,        1, 1, 0,    // Segment 0 (top)
            0, 1, 0,        0, 0.5f, 0, // Segment 1 (top-left)
            1, 1, 0,        1, 0.5f, 0, // Segment 2 (top-right)
            0, 0.5f, 0,     1, 0.5f, 0, // Segment 3 (middle)
            0, 0.5f, 0,     0, 0, 0,    // Segment 4 (bottom-left)
            1, 0.5f, 0,     1, 0, 0,    // Segment 5 (bottom-right)
            0, 0, 0,        1, 0, 0,    // Segment 6 (bottom)
        };

        internal DrawNumbers(int maxDigits, DrawData drawData)
        {
            // Every Digit could have maximum 7 lines so we just allocate maximum 7:
            /*
                     --0--
                    |     |
                   1|     |2
                    |     |
                     --3--
                    |     |
                   4|     |5
                    |     |
                     --6--
            */
            var maxLines = 7;
            m_Unit = drawData.m_LineBuffer.AllocateAtomic(maxDigits * maxLines);
            m_DrawData = drawData;
        }

        internal unsafe void DrawNumber(int number, float3 position, float digitSpacing, float scale, ColorIndex color)
        {
            // Maximum 10 digits for a positive 32-bit integer
            int maxDigits = 10;
            int* digits = stackalloc int[maxDigits];
            int digitCount = 0;

            if (number == 0)
            {
                digits[0] = 0;
                digitCount = 1;
            }
            else
            {
                int n = number;
                while (n > 0 && digitCount < maxDigits)
                {
                    digits[digitCount++] = n % 10;
                    n /= 10;
                }
            }

            // Draw digits in reverse order (left to right)
            for (int i = 0; i < digitCount; i++)
            {
                int digit = digits[digitCount - i - 1];
                float3 digitOffset = new float3(i * digitSpacing, 0, 0);
                DrawDigit(digit, position + digitOffset, scale, color);
            }
        }

        void DrawDigit(int digit, float3 basePos, float scale, ColorIndex color)
        {
            unsafe
            {
                int* segments = stackalloc int[k_MaxSegments];
                GetDigitSegments(digit, segments, out int segmentCount);
                for (int i = 0; i < segmentCount; i++)
                {
                    int idx = segments[i];
                    float3 a = basePos + new float3(s_SegmentPoints[idx * 6], s_SegmentPoints[idx * 6 + 1], s_SegmentPoints[idx * 6 + 2]) * scale;
                    float3 b = basePos + new float3(s_SegmentPoints[idx * 6 + 3], s_SegmentPoints[idx * 6 + 4], s_SegmentPoints[idx * 6 + 5]) * scale;
                    DrawLine(a, b, color);
                }
            }
        }

        void DrawLine(float3 begin, float3 end, ColorIndex color)
        {
            if (m_Unit.m_Next < m_Unit.m_End)
            {
                m_DrawData.m_LineBuffer.SetLine(begin, end, color, m_Unit.m_Next++);
            }
        }

        public void Dispose()
        {
            while (m_Unit.m_Next < m_Unit.m_End)
                m_DrawData.m_LineBuffer.ClearLine(m_Unit.m_Next++);
        }
    }

    //--------
    internal struct DrawLetters : IDisposable
    {
        Unit m_Unit;
        DrawData m_DrawData;

        static readonly FixedList32Bytes<byte> s_Alphabet = new FixedList32Bytes<byte>
        {
            (byte)'A', (byte)'B', (byte)'C', (byte)'D', (byte)'E', (byte)'F', (byte)'G',
            (byte)'H', (byte)'I', (byte)'J', (byte)'K', (byte)'L', (byte)'M', (byte)'N',
            (byte)'O', (byte)'P', (byte)'Q', (byte)'R', (byte)'S', (byte)'T', (byte)'U',
            (byte)'V', (byte)'W', (byte)'X', (byte)'Y', (byte)'Z',
            (byte)':', // index 26
            (byte)' ' // index 27
        };

        const int k_MaxSegmentCount = 6;
        const int k_MaxLetterCount = 28;

        // Each letter maps to a list of segment indices (like a 7-segment display but for letters)
        /*
             --0--
            |     |
            1|     |2
            |     |
                --3--
            |     |
            4|     |5
            |     |
             --6--
        */
        static unsafe void GetLetterSegments(in int letterIndex, int* segments, out int segmentCount)
        {
            var s = segments;
            switch (letterIndex)
            {
                case 0: // A
                {
                    s[0] = 0; s[1] = 1; s[2] = 2; s[3] = 3; s[4] = 4; s[5] = 5;
                    segmentCount = 6;
                    break;
                }
                case 1: // B
                {
                    s[0] = 0; s[1] = 1; s[2] = 3; s[3] = 4; s[4] = 5; s[5] = 6;
                    segmentCount = 6;
                    break;
                }
                case 2: // C
                {
                    s[0] = 0; s[1] = 1; s[2] = 4; s[3] = 6;
                    segmentCount = 4;
                    break;
                }
                case 3: // D
                {
                    s[0] = 0; s[1] = 1; s[2] = 2; s[3] = 4; s[4] = 5; s[5] = 6;
                    segmentCount = 6;
                    break;
                }
                case 4: // E
                {
                    s[0] = 0; s[1] = 1; s[2] = 3; s[3] = 4; s[4] = 6;
                    segmentCount = 5;
                    break;
                }
                case 5: // F
                {
                    s[0] = 0; s[1] = 1; s[2] = 3; s[3] = 4;
                    segmentCount = 4;
                    break;
                }
                case 6: // G
                {
                    s[0] = 0; s[1] = 1; s[2] = 2; s[3] = 4; s[4] = 5; s[5] = 6;
                    segmentCount = 6;
                    break;
                }
                case 7: // H
                {
                    s[0] = 1; s[1] = 2; s[2] = 3; s[3] = 4; s[4] = 5;
                    segmentCount = 5;
                    break;
                }
                case 8: // I
                {
                    s[0] = 2; s[1] = 5;
                    segmentCount = 2;
                    break;
                }
                case 9: // J
                {
                    s[0] = 2; s[1] = 4; s[2] = 5; s[3] = 6;
                    segmentCount = 4;
                    break;
                }
                case 10: // K (kind of approximate)
                {
                    s[0] = 1; s[1] = 3; s[2] = 4; s[3] = 5;
                    segmentCount = 4;
                    break;
                }
                case 11: // L
                {
                    s[0] = 1; s[1] = 4; s[2] = 6;
                    segmentCount = 3;
                    break;
                }
                case 12: // M (kind of approximate)
                {
                    s[0] = 0; s[1] = 2; s[2] = 4; s[3] = 5;
                    segmentCount = 4;
                    break;
                }
                case 13: // N
                {
                    s[0] = 0; s[1] = 2; s[2] = 3; s[3] = 4; s[4] = 5;
                    segmentCount = 5;
                    break;
                }
                case 14: // O
                {
                    s[0] = 0; s[1] = 1; s[2] = 2; s[3] = 4; s[4] = 5; s[5] = 6;
                    segmentCount = 6;
                    break;
                }
                case 15: // P
                {
                    s[0] = 0; s[1] = 1; s[2] = 2; s[3] = 3; s[4] = 4;
                    segmentCount = 5;
                    break;
                }
                case 16: // Q
                {
                    s[0] = 0; s[1] = 1; s[2] = 2; s[3] = 3; s[4] = 5; s[5] = 6;
                    segmentCount = 6;
                    break;
                }
                case 17: // R
                {
                    s[0] = 0; s[1] = 1; s[2] = 2; s[3] = 3; s[4] = 4; s[5] = 5;
                    segmentCount = 6;
                    break;
                }
                case 18: // S
                {
                    s[0] = 0; s[1] = 1; s[2] = 3; s[3] = 5; s[4] = 6;
                    segmentCount = 5;
                    break;
                }
                case 19: // T
                {
                    s[0] = 1; s[1] = 3; s[2] = 6;
                    segmentCount = 3;
                    break;
                }
                case 20: // U
                {
                    s[0] = 1; s[1] = 2; s[2] = 4; s[3] = 5; s[4] = 6;
                    segmentCount = 5;
                    break;
                }
                case 21: // V (kind of approximate)
                {
                    s[0] = 1; s[1] = 4; s[2] = 6;
                    segmentCount = 3;
                    break;
                }
                case 22: // W (kind of approximate)
                {
                    s[0] = 1; s[1] = 2; s[2] = 4; s[3] = 5; s[4] = 6;
                    segmentCount = 5;
                    break;
                }
                case 23: // X
                {
                    s[0] = 1; s[1] = 2; s[2] = 3; s[3] = 4; s[4] = 5;
                    segmentCount = 5;
                    break;
                }
                case 24: // Y
                {
                    s[0] = 1; s[1] = 2; s[2] = 3; s[3] = 5; s[4] = 6;
                    segmentCount = 5;
                    break;
                }
                case 25: // Z
                {
                    s[0] = 0; s[1] = 2; s[2] = 3; s[3] = 4; s[4] = 6;
                    segmentCount = 5;
                    break;
                }
                case 26: // ':' -> 2 short center dots
                {
                    s[0] = 7; s[1] = 8;
                    segmentCount = 2;
                    break;
                }
                case 27: // ' ' -> no segments
                default:
                {
                    segmentCount = 0;
                    break;
                }
            }
        }

        static readonly float[] s_SegmentPoints =
        {
            0, 1, 0,        1, 1, 0,        // Segment 0
            0, 1, 0,        0, 0.5f, 0,     // Segment 1
            1, 1, 0,        1, 0.5f, 0,     // Segment 2
            0, 0.5f, 0,     1, 0.5f, 0,     // Segment 3
            0, 0.5f, 0,     0, 0, 0,        // Segment 4
            1, 0.5f, 0,     1, 0, 0,        // Segment 5
            0, 0, 0,        1, 0, 0,        // Segment 6
            0.4f, 0.75f, 0, 0.6f, 0.75f, 0, // Segment 7 - upper dot
            0.4f, 0.25f, 0, 0.6f, 0.25f, 0, // Segment 8 - lower dot
        };

        internal DrawLetters(int maxChars, DrawData drawData)
        {
            const int maxSegmentsPerChar = 7;
            m_Unit = drawData.m_LineBuffer.AllocateAtomic(maxChars * maxSegmentsPerChar);
            m_DrawData = drawData;
        }

        internal void DrawText(in FixedString32Bytes text, float3 position, float spacing, float scale, ColorIndex color)
        {
            for (int i = 0; i < text.Length; i++)
            {
                byte ch = text[i];
                int letterIndex = IndexOf(ch);
                if (letterIndex >= 0 && letterIndex < k_MaxLetterCount)
                {
                    float3 offset = new float3(i * spacing, 0, 0);
                    DrawChar(letterIndex, position + offset, scale, color);
                }
            }
        }

        void DrawChar(int letterIndex, float3 basePos, float scale, ColorIndex color)
        {
            unsafe
            {
                int* segments = stackalloc int[k_MaxSegmentCount];
                GetLetterSegments(letterIndex, segments, out int segmentCount);
                for (int i = 0; i < segmentCount; i++)
                {
                    int idx = segments[i];
                    float3 a = basePos + new float3(s_SegmentPoints[idx * 6], s_SegmentPoints[idx * 6 + 1], s_SegmentPoints[idx * 6 + 2]) * scale;
                    float3 b = basePos + new float3(s_SegmentPoints[idx * 6 + 3], s_SegmentPoints[idx * 6 + 4], s_SegmentPoints[idx * 6 + 5]) * scale;
                    DrawLine(a, b, color);
                }
            }
        }

        void DrawLine(float3 a, float3 b, ColorIndex color)
        {
            if (m_Unit.m_Next < m_Unit.m_End)
                m_DrawData.m_LineBuffer.SetLine(a, b, color, m_Unit.m_Next++);
        }

        public void Dispose()
        {
            while (m_Unit.m_Next < m_Unit.m_End)
                m_DrawData.m_LineBuffer.ClearLine(m_Unit.m_Next++);
        }

        static int IndexOf(byte ch)
        {
            for (int i = 0; i < s_Alphabet.Length; i++)
                if (s_Alphabet[i] == ch)
                    return i;
            return -1;
        }
    }
}

#endif
