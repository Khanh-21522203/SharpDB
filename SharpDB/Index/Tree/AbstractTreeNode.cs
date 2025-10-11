using System.Collections;
using System.Collections.Immutable;
using SharpDB.DataStructure;
using SharpDB.Index.Binary;
using SharpDB.Utils;

namespace SharpDB.Index.Tree;

public enum NodeType
{
    Leaf,
    Internal
}

public abstract class AbstractTreeNode<TK>(
    INdexBinaryObjectFactory<TK> kIndexBinaryObjectFactory,
    byte[] data) where TK : IComparable<TK>
{
    public const byte TypeLeafNodeBit = 0x02; // 0 0 1 0
    public const byte TypeInternalNodeBit = 0x01; // 0 0 0 1
    public const byte RootBit = 0x04; // 0 1 0 0

    private Pointer _pointer { set; get; }
    protected readonly INdexBinaryObjectFactory<TK> KIndexBinaryObjectFactory = kIndexBinaryObjectFactory;

    public bool IsLeaf() => (data[0] & TypeLeafNodeBit) == TypeLeafNodeBit;
    public bool IsRoot() => (data[0] & RootBit) == RootBit;
    public bool IsInternal() => (data[0] & TypeInternalNodeBit) == TypeInternalNodeBit;

    public void SetAsRoot()
    {
        data[0] = (byte)(data[0] | RootBit);
    }

    public void UnsetAsRoot()
    {
        data[0] = (byte)(data[0] & ~RootBit);
    }

    public NodeType GetType()
    {
        if ((data[0] & TypeLeafNodeBit) == TypeLeafNodeBit)
            return NodeType.Leaf;
        if ((data[0] & TypeInternalNodeBit) == TypeInternalNodeBit)
            return NodeType.Internal;

        throw new InvalidOperationException("Invalid node type");
    }

    public byte[] GetData() => data;

    // Phương thức GetKeys đơn giản hóa với yield return
    public IEnumerable<TK> GetKeys(int degree, int valueSize)
    {
        for (int cursor = 0; cursor < degree; cursor++)
        {
            // Kiểm tra xem có key hợp lệ tại index cursor không
            if (NodeUtils.HasKeyAtIndex(this, cursor, degree, KIndexBinaryObjectFactory, valueSize))
            {
                var indexBinaryObject = NodeUtils.GetKeyAtIndex(this, cursor, KIndexBinaryObjectFactory, valueSize);
                yield return indexBinaryObject.AsObject();
            }
            else
            {
                // Dừng nếu không còn key (giả sử key liền kề)
                yield break;
            }
        }
    }

    public ImmutableList<TK> GetKeyList(int degree, int valueSize)
    {
        return ImmutableList.CreateRange(GetKeys(degree, valueSize));
    }

    public void SetKey(int index, TK key, int valueSize)
    {
        NodeUtils.SetKeyAtIndex(this, index, KIndexBinaryObjectFactory.Create(key), valueSize);
    }

    public KVSize GetKVSize()
    {
        return new KVSize(KIndexBinaryObjectFactory.Size(), PointerIndexBinaryObject.BYTES);
    }

    public void RemoveKey(int idx, int degree, int valueSize)
    {
        var keyList = GetKeyList(degree, valueSize);
        NodeUtils.RemoveKeyAtIndex(this, idx, KIndexBinaryObjectFactory.Size(), valueSize);
        var subList = keyList.GetRange(idx + 1, keyList.Count - (idx + 1));
        int lastIndex = -1;
        for (int i = 0; i < subList.Count; i++)
        {
            lastIndex = idx + i;
            NodeUtils.SetKeyAtIndex(
                this,
                lastIndex,
                KIndexBinaryObjectFactory.Create(subList[i]),
                valueSize
            );
        }

        if (lastIndex != -1)
        {
            for (int i = lastIndex + 1; i < degree - 1; i++)
            {
                NodeUtils.RemoveKeyAtIndex(this, i, KIndexBinaryObjectFactory.Size(), valueSize);
            }
        }
    }
}