using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;
using SharpDB.Index.Node;
using SharpDB.Index.Manager;
using SharpDB.DataStructures;
using SharpDB.Storage.Index;
using SharpDB.Storage.FilePool;
using SharpDB.Configuration;
using Serilog;

namespace SharpDB.Test.Index;

public class InternalNodeSplitTests : IDisposable
{
    private readonly string _testPath;
    private readonly BPlusTreeIndexManager<long, Pointer> _index;

    public InternalNodeSplitTests()
    {
        _testPath = Path.Combine(Path.GetTempPath(), $"splitests-{Guid.NewGuid()}");
        Directory.CreateDirectory(_testPath);

        var logger = new LoggerConfiguration()
            .MinimumLevel.Warning()
            .CreateLogger();

        var config = new EngineConfig();
        var filePool = new FileHandlerPool(logger, config);
        var indexStorage = new DiskPageFileIndexStorageManager(_testPath, logger, filePool);

        _index = new BPlusTreeIndexManager<long, Pointer>(
            storage: indexStorage,
            indexId: 1,
            degree: 5); // Very small degree to force splits
    }

    [Fact]
    public async Task Split_Should_Not_Corrupt_Keys()
    {
        // Arrange: Insert enough keys to trigger multiple splits
        // With degree=5, node splits at 5 keys
        for (long i = 0; i < 100; i++)
        {
            var pointer = new Pointer(Pointer.TypeData, i * 100 + 1, 1);
            await _index.PutAsync(i, pointer);
        }

        await _index.FlushAsync();

        // Assert: All keys should still be retrievable after splits
        var notFound = new System.Collections.Generic.List<long>();
        for (long i = 0; i < 100; i++)
        {
            var pointer = await _index.GetAsync(i);
            if (pointer.Position == 0)
            {
                notFound.Add(i);
            }
        }

        Assert.Empty(notFound);
    }

    [Fact]
    public async Task Split_Multiple_Times_All_Keys_Retrievable()
    {
        // Arrange: Force many splits with small degree
        for (long i = 0; i < 50; i++)
        {
            var pointer = new Pointer(Pointer.TypeData, i * 100 + 1, 1);
            await _index.PutAsync(i, pointer);
        }

        await _index.FlushAsync();

        // Assert: Verify all keys
        for (long i = 0; i < 50; i++)
        {
            var pointer = await _index.GetAsync(i);
            Assert.NotEqual(0, pointer.Position);
            Assert.Equal(i * 100 + 1, pointer.Position);
        }
    }

    [Fact]
    public async Task Split_Does_Not_Lose_Edge_Keys()
    {
        // Arrange: Test edge cases - first and last keys
        var testKeys = new long[] { 0, 1, 10, 49, 50, 99, 100 };
        
        for (long i = 0; i <= 100; i++)
        {
            var pointer = new Pointer(Pointer.TypeData, i * 100 + 1, 1);
            await _index.PutAsync(i, pointer);
        }

        await _index.FlushAsync();

        // Assert: Edge keys should be retrievable
        foreach (var key in testKeys)
        {
            var pointer = await _index.GetAsync(key);
            Assert.NotEqual(0, pointer.Position);
            Assert.Equal(key * 100 + 1, pointer.Position);
        }
    }

    [Fact]
    public async Task Split_With_Sequential_Insert_No_Ghost_Keys()
    {
        // Arrange: Sequential insert (common case)
        for (long i = 0; i < 30; i++)
        {
            var pointer = new Pointer(Pointer.TypeData, i + 1000, 1);
            await _index.PutAsync(i, pointer);
        }

        await _index.FlushAsync();

        // Assert: Count should match inserted count
        var count = await _index.CountAsync();
        Assert.Equal(30, count);

        // Assert: All keys retrievable
        for (long i = 0; i < 30; i++)
        {
            var pointer = await _index.GetAsync(i);
            Assert.NotEqual(0, pointer.Position);
            Assert.Equal(i + 1000, pointer.Position);
        }
    }

    [Fact]
    public async Task Split_With_Reverse_Insert_No_Ghost_Keys()
    {
        // Arrange: Reverse insert (stress test for split)
        for (long i = 29; i >= 0; i--)
        {
            var pointer = new Pointer(Pointer.TypeData, i + 2000, 1);
            await _index.PutAsync(i, pointer);
        }

        await _index.FlushAsync();

        // Assert: Count should match
        var count = await _index.CountAsync();
        Assert.Equal(30, count);

        // Assert: All keys retrievable
        for (long i = 0; i < 30; i++)
        {
            var pointer = await _index.GetAsync(i);
            Assert.NotEqual(0, pointer.Position);
            Assert.Equal(i + 2000, pointer.Position);
        }
    }

    [Fact]
    public async Task After_Split_Left_Node_Keys_Still_Accessible()
    {
        // Arrange: Insert keys that will cause split
        // With degree=5, insert 0-9 (will split)
        for (long i = 0; i < 10; i++)
        {
            var pointer = new Pointer(Pointer.TypeData, i + 5000, 1);
            await _index.PutAsync(i, pointer);
        }

        await _index.FlushAsync();

        // Assert: Keys in left node after split should be accessible
        // Assuming split at midpoint, keys 0-4 should be in left
        for (long i = 0; i < 5; i++)
        {
            var pointer = await _index.GetAsync(i);
            Assert.NotEqual(0, pointer.Position);
            Assert.Equal(i + 5000, pointer.Position);
        }
    }

    [Fact]
    public async Task After_Split_Right_Node_Keys_Still_Accessible()
    {
        // Arrange: Insert keys that will cause split
        for (long i = 0; i < 10; i++)
        {
            var pointer = new Pointer(Pointer.TypeData, i + 6000, 1);
            await _index.PutAsync(i, pointer);
        }

        await _index.FlushAsync();

        // Assert: Keys in right node after split should be accessible
        // Keys 5-9 should be in right node
        for (long i = 5; i < 10; i++)
        {
            var pointer = await _index.GetAsync(i);
            Assert.NotEqual(0, pointer.Position);
            Assert.Equal(i + 6000, pointer.Position);
        }
    }

    [Fact]
    public async Task After_Split_Middle_Key_Promoted_Not_Lost()
    {
        // Arrange: The middle key gets promoted to parent
        // It should still be searchable
        for (long i = 0; i < 15; i++)
        {
            var pointer = new Pointer(Pointer.TypeData, i + 7000, 1);
            await _index.PutAsync(i, pointer);
        }

        await _index.FlushAsync();

        // Assert: All keys including middle keys should be accessible
        for (long i = 0; i < 15; i++)
        {
            var pointer = await _index.GetAsync(i);
            Assert.NotEqual(0, pointer.Position);
        }
    }

    public void Dispose()
    {
        if (Directory.Exists(_testPath))
        {
            Directory.Delete(_testPath, true);
        }
    }
}
