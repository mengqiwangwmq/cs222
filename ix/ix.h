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
    RC split(vector<Node*> &route, IXFileHandle &ixFileHandle);
    RC constructRouteToLeaf(IXFileHandle &ixFileHandle, vector<Node *> &route, Node *root, const void *key, const Attribute &attribute);

    // Delete an entry from the given index that is indicated by the given ixFileHandle.
    RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

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
    void printTreeElement(IXFileHandle &ixFileHandle,  const Attribute &attribute, int &pageNum, int indent) const;

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
    ~IX_ScanIterator();

    // Get next matching entry
    RC getNextEntry(RID &rid, void *key);

    // Traverse to leaf node
    int reachLeaf(Node *&node);

    // Terminate index scan
    RC close();

    int cPage;
    int curK;
    int curR;
    int prevP;
    int prevK;
    int prevR;
    RID prevRid;

    const void *lowKey;
    const void *highKey;
    bool lowKeyInclusive;
    bool highKeyInclusive;

    IXFileHandle *ixFileHandle;
    Node *node = nullptr;
    const Attribute *attribute;
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

typedef enum { Root = 0, Intermediate, Leaf, RootLeaf } NodeType;

class Node {
public :
    NodeType nodeType;
    AttrType attrType;
    const Attribute *attribute;
    vector<void *> keys;
    vector<int> children; //pointers to children
    vector<vector<RID>> pointers; //where the record lies
    int next = -1;
    int previous = -1;
    int cPage = -1;
    //int order = 2;
    bool isOverflow = false;
    vector<int> overFlowPages;
    int size = 0;

    Node(const Attribute *attribute, const void* page, IXFileHandle *ixfileHandle);
    Node(const Attribute &attribute);
    Node(const Attribute *attribute);
    ~Node();
    RC serialize(void *page);
    int serializeOverflowPage(int start, int end, void* page);
    RC deserializeOverflowPage(int nodeId, IXFileHandle *ixfileHandle);
    RC insert(void* key, RID rid);
    RC insert(void* key, int child);
    int insertKey(int &pos, const void* key);
    RC insertChild(const int &pos, int &pageNum);
    RC insertPointer(int pos, const bool &exist, const RID &rid);
    RC printNodeKeys();
    RC printNodePointers(int indent);
    int getNodeSize();
    bool compareEqual(const void *compValue, const void *compKey);
    int compareLess(const void *compValue, const void *compKey);
    int compareLarge(const void *compValue, const void *compKey);
    RC locateChildPos(int &pos, bool &exist, const void * value);
    int calHeaderSize();
    RC serializeNode(IXFileHandle &ixfileHandle);
    int deleteRecord(int pos, const RID &rid);
};

#endif
