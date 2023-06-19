#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

typedef char byte;

typedef enum NodeType
{
    Leaf,
    Extension,
    Branch,
} NodeType;

const byte IsDirtyFlag = 0b1000;
const byte NodeTypeFlag = 0b0110;

typedef struct MerkleNode
{
    byte Header;     // Contains 'Dirty' and 'NodeType'
    byte Keccak[32]; // We *don't* want Keccak for 'Extension' type
    byte Length;     // Length of 'Data'
    byte *Data;      // Raw 'Data' (ex. nibble bitset on 'Branch', path on 'Leaf')
} MerkleNode;

bool MerkleNode_IsDirty(MerkleNode node)
{
    return (node.Header & IsDirtyFlag) != 0;
}

NodeType MerkleNode_NodeType(MerkleNode node)
{
    return (NodeType)((node.Header & NodeTypeFlag) >> 1);
}

MerkleNode MerkleNode_newBranch(byte *nibbles)
{
    MerkleNode branch = {};
    branch.Header = (byte)Branch << 1 | IsDirtyFlag;
    memset(branch.Keccak, 0, sizeof(branch.Keccak));
    branch.Length = 2;
    branch.Data = malloc(branch.Length);
    memcpy(branch.Data, nibbles, branch.Length);

    return branch;
}

MerkleNode MerkleNode_newExtension(byte *nibblePath, int pathLength)
{
    MerkleNode extension = {};
    extension.Header = (byte)Extension << 1 | IsDirtyFlag;
    memset(extension.Keccak, 0, sizeof(extension.Keccak));
    extension.Length = pathLength;
    extension.Data = malloc(extension.Length);
    memcpy(extension.Data, nibblePath, extension.Length);

    return extension;
}

MerkleNode MerkleNode_newLeaf(byte *nibblePath, int pathLength)
{
    MerkleNode leaf = {};
    leaf.Header = (byte)Leaf << 1 | IsDirtyFlag;
    memset(leaf.Keccak, 0, sizeof(leaf.Keccak));
    leaf.Length = pathLength;
    leaf.Data = malloc(leaf.Length);
    memcpy(leaf.Data, nibblePath, leaf.Length);

    return leaf;
}

void print_array(byte *array, int length)
{
    if (length == 0)
    {
        printf("[]\n");
        return;
    }
    else
    {
        printf("[");
        for (int i = 0; i < length - 1; i++)
        {
            printf("%x, ", array[i]);
        }
        printf("%x]\n", array[length - 1]);
    }
}

void test_branch()
{
    byte nibbles[] = {0b01101001, 0b01101001};
    MerkleNode branch = MerkleNode_newBranch(nibbles);

    printf("branch.IsDirty: %d\n", MerkleNode_IsDirty(branch));
    printf("branch.NodeType: %d\n", MerkleNode_NodeType(branch));
    printf("branch.Length: %d\n", branch.Length);
    print_array(branch.Data, branch.Length);
}

void test_extension()
{
    byte path[] = {0x1, 0x3, 0x5, 0x7};
    MerkleNode extension = MerkleNode_newExtension(path, sizeof(path));

    printf("extension.IsDirty: %d\n", MerkleNode_IsDirty(extension));
    printf("extension.NodeType: %d\n", MerkleNode_NodeType(extension));
    printf("extension.Length: %d\n", extension.Length);
    print_array(extension.Data, extension.Length);
}

void test_leaf()
{
    byte path[] = {0x2, 0x4, 0x6};
    MerkleNode leaf = MerkleNode_newLeaf(path, sizeof(path));

    printf("leaf.IsDirty: %d\n", MerkleNode_IsDirty(leaf));
    printf("leaf.NodeType: %d\n", MerkleNode_NodeType(leaf));
    printf("leaf.Length: %d\n", leaf.Length);
    print_array(leaf.Data, leaf.Length);
}

int main(void)
{
    test_leaf();
    test_extension();
    test_branch();

    return 0;
}
