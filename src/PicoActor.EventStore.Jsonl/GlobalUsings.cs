// The store's read path is byte-shaped (little-endian snapshot headers, '\n' framing) and its whole
// state is a bounded table of gates plus counters — BinaryPrimitives and ConcurrentDictionary are
// not in the implicit usings.
global using System.Buffers.Binary;
global using System.Collections.Concurrent;
global using System.Text;
global using PicoActor.Abs;
global using PicoJetson;
global using PicoLog.Abs;
