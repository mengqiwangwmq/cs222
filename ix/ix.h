#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

class Node;

class IX_ScanIterator;

class IXFileHandle;

class IndexManager {

public:
    static IndexManager &instance();

    // Create an index file.
    RC createFile(const std::string &fileName);

    // Delete an index file.
    RC destroyFile(const std::string &fileName);

    // Open an index and return an ixFileHandle.
    RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

    // Close an ixFileHandle for an index.
    RC closeFile(IXFileHandle &ixFileHandle);

    // Insert an entry into the given index that is indicated by the given ixFileHandle.
    RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

    RC split(IXFileHandle &ixFileHandle, vector<Node *> &route);

    RC routeToLeaf(IXFileHandle &ixFileHandle, vector<Node *> &route, Node *root, AttrValue &attrValue);

    // Delete an entry from the given index that is indicated by the given ixFileHandle.
    RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

    RC checkMerge(IXFileHandle &ixFileHandle, vector<Node *> &route);

    RC merge(IXFileHandle &ixFileHandle, Node *sibNode, vector<Node *> &route, int &pos, int &siblingType);

    RC borrow(IXFileHandle &ixFileHandle, Node *node, Node *sibNode, Node *parent, int &pos, int &siblingType);

    // Initialize and IX_ScanIterator to support a range search
    RC scan(IXFileHandle &ixFileHandle,
            const Attribute &attribute,
            const void *lowKey,
            const void *highKey,
            bool lowKeyInclusive,
            bool highKeyInclusive,
            IX_ScanIterator &ix_ScanIterator);

    // Print the B+ tree in pre-order (in a JSON record format)
    void printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const;

    void printTreeNode(IXFileHandle &ixFileHandle, const Attribute &attribute, int &pageNum, int indent) const;

protected:
    IndexManager() = default;                                                   // Prevent construction
    ~IndexManager() = default;                                                  // Prevent unwanted destruction
    IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
    IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment
    PagedFileManager  *pfm;
};

class IX_ScanIterator {
public:

    // Constructor
    IX_ScanIterator();

    // Destructor
    ~IX_ScanIterator() = default;

    // Get next matching entry
    RC getNextEntry(RID &rid, void *key);

    // Traverse to leaf node
    int reachLeaf();

    // Terminate index scan
    RC close();

    int pageNum;
    int curK;
    int curR;
    int prevP;
    int prevK;
    int prevR;
    RID prevRid;

    AttrValue lowKey;
    AttrValue highKey;
    bool lowKeyInclusive;
    bool highKeyInclusive;

    IXFileHandle *ixFileHandle;
    Node *node = nullptr;
    AttrType attrType;
};

class IXFileHandle {
public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    FileHandle fileHandle;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    // Put the current counter values of associated PF FileHandles into variables
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

};

typedef enum {
    Root = 0, Intermediate, Leaf, SingleRoot
} NodeType;

class Node {
public :
    NodeType nodeType;
    AttrType attrType;
    vector<AttrValue> keys;
    vector<int> children; //pointers to children
    vector<vector<RID> > pointers; //where the record lies
    int next = -1;
    int previous = -1;
    int pageNum = -1;
    bool isOverflow;
    vector<int> overFlowPages;
    int size = 0;

    Node(IXFileHandle &ixFileHandle, AttrType type, const void *page);

    Node(AttrType type);
    ~Node();
    RC serialize(void *page);
    int serializeOverflowPage(int start, int end, void* page);

    RC deserializeOverflowPage(IXFileHandle &ixFileHandle, int nodePageNum);

    int locateChildPos(AttrValue &attrValue, CompOp compOp);

    bool checkKeyExist(const int &pos, const AttrValue &attrValue);

    void insertKey(const int &pos, const AttrValue &attrValue);

    void insertChild(const int &pos, const int &pid);

    void insertPointer(const int &pos, const AttrValue &attrValue, const RID &rid);

    int getRightSibling(Node *parent, int &pos);

    int getLeftSibling(Node *parent, int &pos);
    int getNodeSize();

    int getHeaderSize();

    RC writeNode(IXFileHandle &ixFileHandle);
    int deleteRecord(int pos, const RID &rid);

    RC printNodeKeys();

    RC printNodePointers(int indent);
};

#endif
