using SharpDB.DataStructure;
using SharpDB.Index.Binary;
using SharpDB.Index.Tree;

namespace SharpDB.Utils;

/// <summary>
/// Utility class cung cấp các phương thức static để thao tác với tree node trong B+ tree implementation.
/// </summary>
public static class NodeUtils
{
    private const int OffsetTreeNodeFlagsEnd = 1;
    private const int OffsetLeafNodeKeyBegin = 1;
    private static readonly int SizeLeafNodeSiblingPointers = 2 * Pointer.Bytes;

    /// <summary>
    /// Tính toán offset của child pointer trong mảng byte dựa trên index.
    /// </summary>
    private static int GetChildPointerOffset(int index, int keySize)
    {
        return OffsetTreeNodeFlagsEnd + (index * (Pointer.Bytes + keySize));
    }

    /// <summary>
    /// Tính toán offset bắt đầu của key trong mảng byte dựa trên loại node và index.
    /// </summary>
    private static int GetKeyStartOffset<TK>(AbstractTreeNode<TK> treeNode, int index, int keySize, int valueSize)
        where TK : IComparable<TK>
    {
        if (treeNode.IsLeaf())
        {
            return OffsetTreeNodeFlagsEnd + index * (keySize + valueSize);
        }
        
        return OffsetTreeNodeFlagsEnd + Pointer.Bytes + index * (keySize + Pointer.Bytes);
    }

    // ============ Child Pointer Methods ============

    /// <summary>
    /// Kiểm tra sự tồn tại của child pointer tại index trong internal node.
    /// </summary>
    public static bool HasChildPointerAtIndex<TK>(AbstractTreeNode<TK> node, int index, int keySize) 
        where TK : IComparable<TK>
    {
        int offset = GetChildPointerOffset(index, keySize);
        
        if (offset >= node.GetData().Length)
            return false;
        
        return node.GetData()[offset] == (byte)PointerType.TypeNode;
    }
    
    /// <summary>
    /// Lấy child pointer tại index trong internal node.
    /// </summary>
    public static Pointer GetChildPointerAtIndex<TK>(AbstractTreeNode<TK> node, int index, int keySize)
        where TK : IComparable<TK>
    {
        return Pointer.FromBytes(node.GetData(), GetChildPointerOffset(index, keySize));
    }
    
    /// <summary>
    /// Đặt child pointer tại index với pointer object cho trước trong internal node.
    /// </summary>
    public static void SetChildPointerAtIndex<TK>(AbstractTreeNode<TK> node, int index, Pointer pointer, int keySize)
        where TK : IComparable<TK>
    {
        byte[] pointerBytes = pointer.ToBytes();
        int destinationIndex = index == 0 
            ? OffsetTreeNodeFlagsEnd 
            : GetChildPointerOffset(index, keySize);

        Array.Copy(
            pointerBytes,
            0,
            node.GetData(),
            destinationIndex,
            Pointer.Bytes
        );
    }
    
    /// <summary>
    /// Xóa child pointer tại index bằng cách ghi đè với byte array rỗng.
    /// </summary>
    public static void RemoveChildPointerAtIndex<TK>(AbstractTreeNode<TK> node, int index, int keySize)
        where TK : IComparable<TK>
    {
        byte[] emptyPointer = new byte[Pointer.Bytes];
        Array.Copy(
            emptyPointer,
            0,
            node.GetData(),
            GetChildPointerOffset(index, keySize),
            Pointer.Bytes
        );
    }

    // ============ Key Methods ============

    /// <summary>
    /// Kiểm tra sự tồn tại của key tại index trong tree node.
    /// </summary>
    public static bool HasKeyAtIndex<TK>(
        AbstractTreeNode<TK> treeNode, 
        int index, 
        int degree, 
        INdexBinaryObjectFactory<TK> kIndexBinaryObjectFactory, 
        int valueSize) 
        where TK : IComparable<TK>
    {
        if (index >= degree - 1)
            return false;

        int keySize = kIndexBinaryObjectFactory.Size();
        int keyStartIndex = GetKeyStartOffset(treeNode, index, keySize, valueSize);
        
        if (keyStartIndex + keySize > treeNode.GetData().Length)
            return false;

        return !BinaryUtils.IsAllZeros(treeNode.GetData(), keyStartIndex, keySize) 
               || !BinaryUtils.IsAllZeros(treeNode.GetData(), keyStartIndex + keySize, valueSize);
    }
    
    /// <summary>
    /// Đặt key tại index với index binary object cho trước.
    /// </summary>
    public static void SetKeyAtIndex<TK>(
        AbstractTreeNode<TK> treeNode, 
        int index, 
        INdexBinaryObject<TK> indexBinaryObject, 
        int valueSize) 
        where TK : IComparable<TK>
    {
        int keyStartIndex = GetKeyStartOffset(treeNode, index, indexBinaryObject.Size(), valueSize);
        Array.Copy(
            indexBinaryObject.GetBytes(),
            (long)0,
            treeNode.GetData(),
            keyStartIndex,
            indexBinaryObject.Size()
        );
    }
    
    /// <summary>
    /// Lấy key tại index dưới dạng IndexBinaryObject.
    /// </summary>
    public static INdexBinaryObject<TK> GetKeyAtIndex<TK>(
        AbstractTreeNode<TK> treeNode, 
        int index, 
        INdexBinaryObjectFactory<TK> kIndexBinaryObjectFactory, 
        int valueSize) 
        where TK : IComparable<TK>
    {
        int keyStartIndex = GetKeyStartOffset(treeNode, index, kIndexBinaryObjectFactory.Size(), valueSize);
        return kIndexBinaryObjectFactory.Create(treeNode.GetData(), keyStartIndex);
    }

    /// <summary>
    /// Xóa key tại index bằng cách ghi đè với byte array rỗng.
    /// </summary>
    public static void RemoveKeyAtIndex<TK>(
        AbstractTreeNode<TK> treeNode, 
        int index, 
        int keySize, 
        int valueSize)
        where TK : IComparable<TK>
    {
        byte[] emptyKey = new byte[keySize];
        Array.Copy(
            emptyKey,
            0,
            treeNode.GetData(),
            GetKeyStartOffset(treeNode, index, keySize, valueSize),
            keySize
        );
    }
    
        /// <summary>
    /// Lấy cặp key-value tại index trong leaf node dưới dạng KeyValuePair.
    /// </summary>
    public static KeyValuePair<TK, TV> GetKeyValueAtIndex<TK, TV>(
        AbstractTreeNode<TK> treeNode,
        int index,
        INdexBinaryObjectFactory<TK> kIndexBinaryObjectFactory,
        INdexBinaryObjectFactory<TV> vIndexBinaryObjectFactory)
        where TK : IComparable<TK>
    {
        int keyStartIndex = GetKeyStartOffset(treeNode, index, kIndexBinaryObjectFactory.Size(), vIndexBinaryObjectFactory.Size());
        
        TK key = kIndexBinaryObjectFactory.Create(treeNode.GetData(), keyStartIndex).AsObject();
        TV value = vIndexBinaryObjectFactory.Create(treeNode.GetData(), keyStartIndex + kIndexBinaryObjectFactory.Size()).AsObject();
        
        return new KeyValuePair<TK, TV>(key, value);
    }

    /// <summary>
    /// Đặt cặp key-value tại index trong leaf node.
    /// </summary>
    public static void SetKeyValueAtIndex<TK, TV>(
        AbstractTreeNode<TK> treeNode, 
        int index, 
        INdexBinaryObject<TK> keyInnerObj, 
        INdexBinaryObject<TV> valueInnerObj)
        where TK : IComparable<TK>
    {
        int kvOffset = OffsetLeafNodeKeyBegin + (index * (keyInnerObj.Size() + valueInnerObj.Size()));
        
        // Copy key bytes
        Array.Copy(
            keyInnerObj.GetBytes(),
            (long)0,
            treeNode.GetData(),
            kvOffset,
            keyInnerObj.Size()
        );

        // Copy value bytes
        Array.Copy(
            valueInnerObj.GetBytes(),
            (long)0,
            treeNode.GetData(),
            kvOffset + keyInnerObj.Size(),
            valueInnerObj.Size()
        );
    }

    /// <summary>
    /// Thêm cặp key-value vào leaf node tại vị trí indexToFill, shift các phần tử phía sau.
    /// </summary>
    public static void AddKeyValue<TK, TV>(
        AbstractTreeNode<TK> treeNode,
        int degree,
        INdexBinaryObjectFactory<TK> indexBinaryObjectFactory,
        TK key,
        INdexBinaryObjectFactory<TV> valueIndexBinaryObjectFactory,
        TV value,
        int indexToFill)
        where TK : IComparable<TK>
    {
        int keySize = indexBinaryObjectFactory.Size();
        int valueSize = valueIndexBinaryObjectFactory.Size();
        int max = degree - 1;
        int bufferSize = ((max - indexToFill - 1) * (keySize + valueSize));

        // Backup data sau indexToFill
        byte[] temp = new byte[bufferSize];
        Array.Copy(
            treeNode.GetData(),
            OffsetLeafNodeKeyBegin + (indexToFill * (keySize + valueSize)),
            temp,
            0,
            temp.Length
        );

        // Set key-value tại indexToFill
        SetKeyValueAtIndex(
            treeNode,
            indexToFill,
            indexBinaryObjectFactory.Create(key),
            valueIndexBinaryObjectFactory.Create(value)
        );

        // Restore data đã backup vào vị trí tiếp theo
        Array.Copy(
            temp,
            0,
            treeNode.GetData(),
            OffsetLeafNodeKeyBegin + ((indexToFill + 1) * (keySize + valueSize)),
            temp.Length
        );
    }

    /// <summary>
    /// Xóa cặp key-value tại index trong leaf node và shift các phần tử phía sau lên.
    /// </summary>
    public static void RemoveKeyValueAtIndex<TK>(
        AbstractTreeNode<TK> treeNode, 
        int index, 
        int keySize, 
        int valueSize)
        where TK : IComparable<TK>
    {
        int nextIndexOffset = GetKeyStartOffset(treeNode, index + 1, keySize, valueSize);
        
        if (nextIndexOffset >= treeNode.GetData().Length - SizeLeafNodeSiblingPointers)
        {
            // Đang xóa key-value cuối cùng, không cần shift, chỉ zero out
            Array.Copy(
                new byte[keySize + valueSize],
                0,
                treeNode.GetData(),
                GetKeyStartOffset(treeNode, index, keySize, valueSize),
                keySize + valueSize
            );
        }
        else
        {
            // Shift các phần tử phía sau lên
            int currentOffset = GetKeyStartOffset(treeNode, index, keySize, valueSize);
            int lengthToShift = treeNode.GetData().Length - nextIndexOffset - SizeLeafNodeSiblingPointers;
            
            Array.Copy(
                treeNode.GetData(),
                nextIndexOffset,
                treeNode.GetData(),
                currentOffset,
                lengthToShift
            );
            
            // Zero out vị trí cuối
            Array.Copy(
                new byte[keySize + valueSize],
                0,
                treeNode.GetData(),
                treeNode.GetData().Length - SizeLeafNodeSiblingPointers - (keySize + valueSize),
                keySize + valueSize
            );
        }
    }

    // ============ Sibling Pointer Methods (Leaf Node) ============

    /// <summary>
    /// Lấy previous pointer trong leaf node (trỏ về leaf node phía trước).
    /// </summary>
    public static Pointer? GetPreviousPointer<TK>(
        AbstractTreeNode<TK> treeNode, 
        int degree, 
        int keySize, 
        int valueSize)
        where TK : IComparable<TK>
    {
        int offset = OffsetLeafNodeKeyBegin + ((degree - 1) * (keySize + valueSize));
        
        if (treeNode.GetData()[offset] == 0x0)
        {
            return null;
        }

        return Pointer.FromBytes(treeNode.GetData(), offset);
    }

    /// <summary>
    /// Đặt previous pointer trong leaf node (trỏ về leaf node phía trước).
    /// </summary>
    public static void SetPreviousPointer<TK>(
        AbstractTreeNode<TK> treeNode, 
        int degree, 
        Pointer pointer, 
        int keySize, 
        int valueSize)
        where TK : IComparable<TK>
    {
        Array.Copy(
            pointer.ToBytes(),
            0,
            treeNode.GetData(),
            OffsetLeafNodeKeyBegin + ((degree - 1) * (keySize + valueSize)),
            Pointer.Bytes
        );
    }

    /// <summary>
    /// Lấy next pointer trong leaf node (trỏ tới leaf node phía sau).
    /// </summary>
    public static Pointer? GetNextPointer<TK>(
        AbstractTreeNode<TK> treeNode, 
        int degree, 
        int keySize, 
        int valueSize)
        where TK : IComparable<TK>
    {
        int offset = OffsetLeafNodeKeyBegin + ((degree - 1) * (keySize + valueSize)) + Pointer.Bytes;
        
        if (treeNode.GetData()[offset] == 0x0)
        {
            return null;
        }

        return Pointer.FromBytes(treeNode.GetData(), offset);
    }

    /// <summary>
    /// Đặt next pointer trong leaf node (trỏ tới leaf node phía sau).
    /// </summary>
    public static void SetNextPointer<TK>(
        AbstractTreeNode<TK> treeNode, 
        int degree, 
        Pointer pointer, 
        int keySize, 
        int valueSize)
        where TK : IComparable<TK>
    {
        Array.Copy(
            pointer.ToBytes(),
            0,
            treeNode.GetData(),
            OffsetLeafNodeKeyBegin + ((degree - 1) * (keySize + valueSize)) + Pointer.Bytes,
            Pointer.Bytes
        );
    }

    /// <summary>
    /// Xóa tất cả child pointers trong internal node bằng cách ghi đè với byte array rỗng.
    /// </summary>
    public static void CleanChildrenPointers<TK>(
        InternalTreeNode<TK> treeNode, 
        int degree, 
        int keySize)
        where TK : IComparable<TK>
    {
        int len = ((degree - 1) * (keySize + PointerIndexBinaryObject.Bytes)) + Pointer.Bytes;
        
        Array.Copy(
            new byte[len],
            0,
            treeNode.GetData(),
            OffsetTreeNodeFlagsEnd,
            len
        );
    }
}
