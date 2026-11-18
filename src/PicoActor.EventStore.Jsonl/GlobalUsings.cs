// The store's read path is byte-shaped (little-endian snapshot headers, '\n' framing) and its whole
// state is a bounded table of gates plus counters — BinaryPrimitives and ConcurrentDictionary are
// not in the implicit usings. ExceptionDispatchInfo re-raises a LATCHED failure as the failure it was.
// PicoSerDe.Core carries the format-agnostic generation attributes and the serializer/deserializer
// interfaces the store's own write path names; PicoJetson carries the JSON facade and the generator.
global using System.Buffers.Binary;
global using System.Collections.Concurrent;
global using System.Collections.Frozen;
global using System.Runtime.ExceptionServices;
global using System.Text;
global using PicoActor.Abs;
global using PicoJetson;
global using PicoLog.Abs;
global using PicoSerDe.Core;
