using SharpDB.Core.Abstractions.Concurrency;

namespace SharpDB.Engine.Concurrency;

public class DeadlockDetector
{
    private readonly Dictionary<TransactionId, HashSet<TransactionId>> _waitForGraph = new();

    public void AddWait(TransactionId waiter, TransactionId holder)
    {
        if (!_waitForGraph.ContainsKey(waiter))
            _waitForGraph[waiter] = new HashSet<TransactionId>();

        _waitForGraph[waiter].Add(holder);
    }

    public void RemoveWait(TransactionId waiter)
    {
        _waitForGraph.Remove(waiter);
    }

    public bool DetectDeadlock(out TransactionId? victim)
    {
        var cycles = FindCycles();

        if (cycles.Count > 0)
        {
            victim = SelectVictim(cycles[0]);
            return true;
        }

        victim = null;
        return false;
    }

    private List<List<TransactionId>> FindCycles()
    {
        var cycles = new List<List<TransactionId>>();
        var visited = new HashSet<TransactionId>();
        var recursionStack = new HashSet<TransactionId>();

        foreach (var node in _waitForGraph.Keys)
            if (!visited.Contains(node))
            {
                var path = new List<TransactionId>();
                if (DFS(node, visited, recursionStack, path)) cycles.Add(new List<TransactionId>(path));
            }

        return cycles;
    }

    private bool DFS(
        TransactionId node,
        HashSet<TransactionId> visited,
        HashSet<TransactionId> recursionStack,
        List<TransactionId> path)
    {
        visited.Add(node);
        recursionStack.Add(node);
        path.Add(node);

        if (_waitForGraph.TryGetValue(node, out var neighbors))
            foreach (var neighbor in neighbors)
                if (!visited.Contains(neighbor))
                {
                    if (DFS(neighbor, visited, recursionStack, path))
                        return true;
                }
                else if (recursionStack.Contains(neighbor))
                {
                    return true; // Cycle detected
                }

        recursionStack.Remove(node);
        path.RemoveAt(path.Count - 1);
        return false;
    }

    private TransactionId SelectVictim(List<TransactionId> cycle)
    {
        // Select youngest transaction (highest ID) as victim
        return cycle.OrderByDescending(t => t.Id).First();
    }
}