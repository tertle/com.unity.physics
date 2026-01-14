using System.Diagnostics;
using System.Globalization;
using System.Text;
using Unity.Mathematics;

namespace Unity.Numerics.Linear.Dense.Primitives
{
    [DebuggerTypeProxy(typeof(VectorDebugView))]
    partial struct Vector {}

    [DebuggerTypeProxy(typeof(MatrixDebugView))]
    partial struct Matrix {}

    internal class VectorDebugView
    {
        private Vector vector;
        public VectorDebugView(Vector v)
        {
            vector = v;
        }

        [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
        public float[] Elements
        {
            get
            {
                return vector.ToArray();
            }
        }

        public string MaximaString
        {
            get
            {
                string s;
                var array = vector.ToArray();
                var parts = new string[array.Length];
                for (int i = 0; i < array.Length; i++)
                {
                    parts[i] = array[i].ToString(CultureInfo.InvariantCulture);
                }
                s = "covect([" + string.Join(",", parts) + "])";
                return s;
            }
        }

        public string MatlabString
        {
            get
            {
                var s = "[";
                var array = vector.ToArray();
                for (int i = 0; i < array.Length; i++)
                {
                    s += array[i].ToString(CultureInfo.InvariantCulture) + "; ";
                }
                if (array.Length > 0)
                    s = s.Substring(0, s.Length - 2);
                s += "]";
                return s;
            }
        }
    }

    internal class MatrixDebugView
    {
        private Matrix matrix;
        public MatrixDebugView(Matrix m)
        {
            matrix = m;
        }

        public (int, int) Dimensions
        {
            get { return (matrix.NumRows, matrix.NumCols); }
        }

        public Vector[] Rows
        {
            get
            {
                var rows = new Vector[matrix.NumRows];

                unsafe
                {
                    for (int i = 0; i < matrix.NumRows; i++)
                        rows[i] = matrix.Rows[i];
                }
                return rows;
            }
        }
        public Vector[] Columns
        {
            get
            {
                var rows = new Vector[matrix.NumCols];
                unsafe
                {
                    for (int i = 0; i < matrix.NumCols; i++)
                        rows[i] = matrix.Cols[i];
                }
                return rows;
            }
        }

        public string MaximaString
        {
            get
            {
                var s = "matrix(";
                unsafe
                {
                    for (int i = 0; i < matrix.NumRows; i++)
                    {
                        var array = matrix.Rows[i].ToArray();
                        s += "[";
                        for (int j = 0; j < array.Length; j++)
                        {
                            s += array[j].ToString(CultureInfo.InvariantCulture);
                            if (j < array.Length - 1)
                                s += ",";
                        }
                        s += "], ";
                    }
                }
                s = s.Substring(0, s.Length - 2) + ")";
                return s;
            }
        }

        public string MatlabString
        {
            get
            {
                var s = "[";
                unsafe
                {
                    for (int i = 0; i < matrix.NumRows; i++)
                    {
                        var array = matrix.Rows[i].ToArray();
                        for (int j = 0; j < array.Length; j++)
                        {
                            s += array[j].ToString(CultureInfo.InvariantCulture);
                            if (j < array.Length - 1)
                                s += ",";
                        }
                        s += "; ";
                    }
                }
                if (matrix.NumRows > 0)
                    s = s.Substring(0, s.Length - 2);
                s += "]";
                return s;
            }
        }
    }
}
