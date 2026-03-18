using SharpDB.Core.Abstractions.Sessions;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.Index.Operations;

namespace SharpDB.Index.Manager;

public sealed class BPlusTreeMutationEngine<TK, TV>(
    InsertOperation<TK, TV> insertOperation,
    DeleteOperation<TK, TV> deleteOperation,
    IIndexIOSession<TK> session,
    IIndexStorageManager storage,
    int indexId)
    : IBPlusTreeMutationEngine<TK, TV>
    where TK : IComparable<TK>
{
    public async Task<MutationResult> MutateAsync(MutationRequest<TK, TV> request, CancellationToken ct = default)
    {
        var beforeRoot = await storage.GetRootPointerAsync(indexId);

        switch (request.Kind)
        {
            case MutationKind.Upsert:
                await insertOperation.InsertAsync(request.Key, request.Value!);
                break;

            case MutationKind.Delete:
                var removed = await deleteOperation.DeleteAsync(request.Key);
                if (!removed)
                    return new MutationResult(false, false, beforeRoot);
                break;

            default:
                throw new NotSupportedException($"Unknown mutation kind: {request.Kind}");
        }

        var afterRoot = await storage.GetRootPointerAsync(indexId);
        return new MutationResult(true, beforeRoot != afterRoot, afterRoot);
    }

    public async Task<CommitResult> CommitAsync(CancellationToken ct = default)
    {
        await session.FlushAsync();
        var root = await storage.GetRootPointerAsync(indexId);
        return new CommitResult(true, root != null, root);
    }
}
