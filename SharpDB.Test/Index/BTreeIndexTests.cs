using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;
using SharpDB.Index.Manager;
using SharpDB.DataStructures;
using SharpDB.Storage.Index;
using SharpDB.Storage.FilePool;
using SharpDB.Configuration;
using Serilog;

namespace SharpDB.Test.Index;

public class BTreeIndexTests : IDisposable
{
    private readonly string _testPath;
    private readonly BPlusTreeIndexManager<long, Pointer> _index;

    public BTreeIndexTests()
    {
        _testPath = Path.Combine(Path.GetTempPath(), $"btreetests-{Guid.NewGuid()}");
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
            degree: 10); // Small degree for testing
    }

    [Fact]
    public async Task Insert_100_Keys_AllShouldBeRetrievable()
    {
        // Arrange & Act: Insert 100 keys
        for (long i = 0; i < 100; i++)
        {
            var pointer = new Pointer(Pointer.TypeData, i * 100 + 1, 1); // +1 to avoid Position=0
            await _index.PutAsync(i, pointer);
        }

        await _index.FlushAsync();

        // Assert: All keys should be retrievable
        for (long i = 0; i < 100; i++)
        {
            var pointer = await _index.GetAsync(i);
            Assert.NotEqual(0, pointer.Position); // Pointer struct should have valid position
            Assert.Equal(i * 100 + 1, pointer.Position);
        }
    }

    [Fact]
    public async Task Insert_1000_Keys_AllShouldBeRetrievable()
    {
        // Arrange & Act: Insert 1000 keys
        for (long i = 0; i < 1000; i++)
        {
            var pointer = new Pointer(Pointer.TypeData, i * 100 + 1, 1); // +1 to avoid Position=0
            await _index.PutAsync(i, pointer);
        }

        await _index.FlushAsync();

        // Assert: All keys should be retrievable
        var notFound = new System.Collections.Generic.List<long>();
        for (long i = 0; i < 1000; i++)
        {
            var pointer = await _index.GetAsync(i);
            if (pointer.Position == 0)
            {
                notFound.Add(i);
            }
        }

        Assert.Empty(notFound); // Should find all keys
    }

    [Fact]
    public async Task Insert_1000_Keys_InReverseOrder_AllShouldBeRetrievable()
    {
        // Arrange & Act: Insert in reverse order
        for (long i = 999; i >= 0; i--)
        {
            var pointer = new Pointer(Pointer.TypeData, i * 100 + 1, 1); // +1 to avoid Position=0
            await _index.PutAsync(i, pointer);
        }

        await _index.FlushAsync();

        // Assert: All keys should be retrievable
        var notFound = new System.Collections.Generic.List<long>();
        for (long i = 0; i < 1000; i++)
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
    public async Task Insert_1000_Keys_CheckSpecificFailingKeys()
    {
        // Arrange & Act: Insert 1000 keys
        for (long i = 0; i < 1000; i++)
        {
            var pointer = new Pointer(Pointer.TypeData, i * 100 + 1, 1); // +1 to avoid Position=0
            await _index.PutAsync(i, pointer);
        }

        await _index.FlushAsync();

        // Assert: Check specific failing keys from our test
        var failingKeys = new long[] { 902, 903, 904, 905, 908, 909, 910, 913, 914, 915 };
        var notFound = new System.Collections.Generic.List<long>();

        foreach (var key in failingKeys)
        {
            var pointer = await _index.GetAsync(key);
            if (pointer.Position == 0)
            {
                notFound.Add(key);
            }
        }

        Assert.Empty(notFound); // These specific keys should be found
    }

    [Fact]
    public async Task Insert_Key_Zero_Alone_ShouldBeRetrievable()
    {
        // Arrange & Act
        var pointer = new Pointer(Pointer.TypeData, 999, 1);
        await _index.PutAsync(0, pointer);
        await _index.FlushAsync();

        // Assert
        var retrieved = await _index.GetAsync(0);
        Assert.NotEqual(0, retrieved.Position);
        Assert.Equal(999, retrieved.Position);
    }

    [Fact]
    public async Task Insert_Key_Zero_With_Other_Keys_ShouldBeRetrievable()
    {
        // Arrange & Act: Insert 0-9
        for (long i = 0; i < 10; i++)
        {
            var pointer = new Pointer(Pointer.TypeData, i * 100 + 1, 1);
            await _index.PutAsync(i, pointer);
        }
        await _index.FlushAsync();

        // Assert: Key 0 should still be retrievable
        var retrieved = await _index.GetAsync(0);
        Assert.NotEqual(0, retrieved.Position);
        Assert.Equal(1, retrieved.Position); // 0 * 100 + 1
    }

    [Fact]
    public async Task Insert_Keys_0_to_20_AllShouldBeRetrievable()
    {
        // Arrange & Act: Insert 0-20 (triggers node split with degree=10)
        for (long i = 0; i <= 20; i++)
        {
            var pointer = new Pointer(Pointer.TypeData, i * 100 + 1, 1);
            await _index.PutAsync(i, pointer);
        }
        await _index.FlushAsync();

        // Assert: All keys including 0 should be retrievable
        var notFound = new System.Collections.Generic.List<long>();
        for (long i = 0; i <= 20; i++)
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
    public async Task Insert_Keys_0_to_50_AllShouldBeRetrievable()
    {
        // Arrange & Act: Insert 0-50 (multiple splits)
        for (long i = 0; i <= 50; i++)
        {
            var pointer = new Pointer(Pointer.TypeData, i * 100 + 1, 1);
            await _index.PutAsync(i, pointer);
        }
        await _index.FlushAsync();

        // Assert: All keys including 0 should be retrievable
        var notFound = new System.Collections.Generic.List<long>();
        for (long i = 0; i <= 50; i++)
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
    public async Task Count_After_1000_Inserts_ShouldBe_1000()
    {
        // Arrange & Act
        for (long i = 0; i < 1000; i++)
        {
            var pointer = new Pointer(Pointer.TypeData, i * 100 + 1, 1); // +1 to avoid Position=0
            await _index.PutAsync(i, pointer);
        }

        await _index.FlushAsync();

        // Assert
        var count = await _index.CountAsync();
        Assert.Equal(1000, count);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testPath))
        {
            Directory.Delete(_testPath, true);
        }
    }
}
