// The store's read path is byte-shaped (little-endian snapshot headers, '\n' framing) and its whole
// state is a bounded table of gates plus counters — BinaryPrimitives and ConcurrentDictionary are
// not in the implicit usings. ExceptionDispatchInfo re-raises a LATCHED failure as the failure it was.
// PicoJetson carries the JSON facade, the generator and the generation attribute the store's generation
// point names (PicoJsonSerializable). PicoSerDe.Core is NOT imported: the store never spells one of its
// types, and leaning on it transitively would turn a future PrivateAssets change upstream into a CS0246.
global using System.Buffers.Binary;
global using System.Collections.Concurrent;
global using System.Collections.Frozen;
global using System.Runtime.ExceptionServices;
global using System.Text;
global using PicoActor.Abs;
global using PicoJetson;
global using PicoLog.Abs;
