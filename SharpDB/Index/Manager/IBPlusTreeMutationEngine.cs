using SharpDB.DataStructures;

namespace SharpDB.Index.Manager;

public enum MutationKind
{
    Upsert,
    Delete
}

public enum MutationCommitMode
{
    Auto,
    Deferred
}

public readonly record struct MutationRequest<TK, TV>(
    MutationKind Kind,
    TK Key,
    TV? Value = default,
    MutationCommitMode CommitMode = MutationCommitMode.Auto);

public readonly record struct MutationResult(bool Applied, bool RootChanged, Pointer? NewRoot);

public readonly record struct CommitResult(bool Flushed, bool RootPersisted, Pointer? RootPointer);

public interface IBPlusTreeMutationEngine<TK, TV>
    where TK : IComparable<TK>
{
    Task<MutationResult> MutateAsync(MutationRequest<TK, TV> request, CancellationToken ct = default);
    Task<CommitResult> CommitAsync(CancellationToken ct = default);
}
