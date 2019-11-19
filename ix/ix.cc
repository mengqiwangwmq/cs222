#include "ix.h"
using namespace std;


IndexManager &IndexManager::instance() {
    static IndexManager _index_manager = IndexManager();
    return _index_manager;
}

RC IndexManager::createFile(const std::string &fileName) {
    return pfm->createFile(fileName);
}

RC IndexManager::destroyFile(const std::string &fileName) {
    return pfm->destroyFile(fileName);
}

RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
    return pfm->openFile(fileName, ixFileHandle.fileHandle);
}

RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
    return pfm->closeFile(ixFileHandle.fileHandle);
}

RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
    if(ixFileHandle.fileHandle.getNumberOfPages() < 1) {
        Node root = Node(attribute);
        root.nodeType = RootLeaf;
        root.cPage = 0;
        int pos = 0;
        root.insertKey(pos, key);
        vector<RID> rids;
        rids.push_back(rid);
        root.pointers.push_back(rids);
        void *page = malloc(PAGE_SIZE);
        ixFileHandle.fileHandle.appendPage(page);
        free(page);
        root.serializeNode(ixFileHandle);
        return 0;
    } else {
        void *page = malloc(PAGE_SIZE);
        ixFileHandle.fileHandle.readPage(0, page);
        Node root = Node(&attribute, page, &ixFileHandle);
        root.cPage = 0;
        free(page);
        if(root.nodeType == RootLeaf) {
            int pos;
            bool exist;
            root.locateChildPos(pos, exist, key);
            if(!exist) {
                root.insertKey(pos, key);
                root.insertPointer(pos, exist, rid);
            } else {
                root.insertPointer(pos-1, exist, rid);
            }
            vector<Node*> route;
            route.push_back(&root);

            if((!exist || root.keys.size() > 1) && root.getNodeSize() > PAGE_SIZE) {
                split(route, ixFileHandle);
            } else {
                root.serializeNode(ixFileHandle);
            }
        } else {
            vector<Node *> route;
            constructRouteToLeaf(ixFileHandle, route, &root, key, attribute);
            Node *leaf = route[route.size()-1];
            int pos;
            bool exist;
            leaf->locateChildPos(pos, exist, key);
            if(!exist) {
                leaf->insertKey(pos, key);
                leaf->insertPointer(pos, exist, rid);
            } else {
                leaf->insertPointer(pos-1, exist, rid);
            }
            if(leaf->keys.size() > 1 && leaf->getNodeSize() > PAGE_SIZE) {
                split(route, ixFileHandle);
            } else {
                leaf->serializeNode(ixFileHandle);
            }
            for(int i = 1; i < route.size(); i ++) {
                delete route[i];
            }
            route.clear();
        }
    }
    return 0;
}

RC IndexManager::split(vector<Node*> &route, IXFileHandle &ixFileHandle) {
    Node *node = route[route.size()-1];
    if(node->nodeType == Leaf) {
        Node newLeaf = Node(node->attribute);
        newLeaf.nodeType = Leaf;
        void *page = malloc(PAGE_SIZE);
        ixFileHandle.fileHandle.appendPage(page);
        newLeaf.cPage = ixFileHandle.fileHandle.getNumberOfPages() - 1;
        free(page);
        newLeaf.next = node->next;
        node->next = newLeaf.cPage;
        newLeaf.previous = node->cPage;
        // TODO: add overflow pages
        // newLeaf->overFlowPages = node->overFlowPages;
        int mid = node->keys.size()/2;
        for(int i = mid; i < node->keys.size(); i ++) {
            newLeaf.keys.push_back(node->keys[i]);
            newLeaf.pointers.push_back(node->pointers[i]);
        }
        node->keys.erase(node->keys.begin()+mid, node->keys.end());
        node->pointers.erase(node->pointers.begin()+mid, node->pointers.end());

        Node *parentNode = route[route.size()-2];
        int pos;
        bool exist;
        parentNode->locateChildPos(pos, exist, newLeaf.keys[0]);
        if(!exist) {
            parentNode->insertKey(pos, newLeaf.keys[0]);
            parentNode->insertChild(pos+1, newLeaf.cPage);
        } else {
            // pos + 1
            parentNode->insertChild(pos, newLeaf.cPage);
        }
        node->serializeNode(ixFileHandle);
        newLeaf.serializeNode(ixFileHandle);
        // Delete the toppest element
        delete route[route.size()-1];
        route.pop_back();

        if(parentNode->keys.size() > 1 && parentNode->getNodeSize() > PAGE_SIZE) {
            split(route, ixFileHandle);
        } else {
            parentNode->serializeNode(ixFileHandle);
        }
    } else if(node->nodeType == Intermediate) {
        Node newInter = Node(node->attribute);
        newInter.nodeType = Intermediate;
        void *page = (char *)malloc(PAGE_SIZE);
        ixFileHandle.fileHandle.appendPage(page);
        newInter.cPage = ixFileHandle.fileHandle.getNumberOfPages()-1;
        free(page);
        // Strictly less than when comes to intermediate node
        int mid = node->keys.size()/2;
        for(int i = mid+1; i < node->keys.size(); i ++) {
            newInter.keys.push_back(node->keys[i]);
            newInter.children.push_back(node->children[i]);
            if(i == node->keys.size()-1) {
                newInter.children.push_back(node->children[i+1]);
            }
        }

        Node *parentNode = route[route.size()-2];
        int pos;
        bool exist;
        parentNode->locateChildPos(pos, exist, node->keys[mid]);
        if(!exist) {
            parentNode->insertKey(pos, node->keys[mid]);
            parentNode->insertChild(pos+1, newInter.cPage);
        } else {
            parentNode->insertChild(pos, newInter.cPage);
        }
        newInter.serializeNode(ixFileHandle);

        node->keys.erase(node->keys.begin()+mid,node->keys.end());
        node->children.erase(node->children.begin()+mid+1, node->children.end());
        node->serializeNode(ixFileHandle);
        delete route[route.size() - 1];
        route.pop_back();
        if((!exist || parentNode->keys.size() > 1) && parentNode->getNodeSize() > PAGE_SIZE)
        {
            split(route, ixFileHandle);
        }
        else
        {
            parentNode->serializeNode(ixFileHandle);
        }
    } else if(node->nodeType == RootLeaf) {
        Node newLeaf1 = Node(node->attribute);
        Node newLeaf2 = Node(node->attribute);
        node->nodeType = Root;
        newLeaf1.nodeType = Leaf;
        newLeaf2.nodeType = Leaf;
        void *page1 = malloc(PAGE_SIZE);
        ixFileHandle.fileHandle.appendPage(page1);
        newLeaf1.cPage = ixFileHandle.fileHandle.getNumberOfPages() - 1;
        void *page2 = malloc(PAGE_SIZE);
        ixFileHandle.fileHandle.appendPage(page2);
        newLeaf2.cPage = ixFileHandle.fileHandle.getNumberOfPages() - 1;
        free(page1);
        free(page2);
        node->children.push_back(newLeaf1.cPage);
        node->children.push_back(newLeaf2.cPage);
        newLeaf1.next = newLeaf2.cPage;
        newLeaf2.previous = newLeaf1.cPage;
        int mid = node->keys.size()/2;
        for(int i = 0; i < node->keys.size(); i ++) {
            if(i >= mid) {
                int size2 = newLeaf2.keys.size();
                newLeaf2.insertKey(size2, node->keys[i]);
                newLeaf2.pointers.push_back(node->pointers[i]);
            }
            if(i < mid) {
                int size1 = newLeaf1.keys.size();
                newLeaf1.insertKey(size1, node->keys[i]);
                newLeaf1.pointers.push_back(node->pointers[i]);
            }
        }
        node->keys.erase(node->keys.begin() + mid+1,node->keys.begin() + node->keys.size());
        node->keys.erase(node->keys.begin(),node->keys.begin() + mid);
        node->pointers.clear();

        // TODO: consider overflow page
        node->serializeNode(ixFileHandle);
        newLeaf1.serializeNode(ixFileHandle);
        newLeaf2.serializeNode(ixFileHandle);
    }else if(node->nodeType == Root) {
        Node newInter1 = Node(node->attribute);
        Node newInter2 = Node(node->attribute);
        newInter1.nodeType = Intermediate;
        newInter2.nodeType = Intermediate;
        void *page1 = malloc(PAGE_SIZE);
        ixFileHandle.fileHandle.appendPage(page1);
        newInter1.cPage = ixFileHandle.fileHandle.getNumberOfPages() - 1;
        void *page2 = malloc(PAGE_SIZE);
        ixFileHandle.fileHandle.appendPage(page2);
        newInter2.cPage = ixFileHandle.fileHandle.getNumberOfPages() - 1;
        free(page1);
        free(page2);
        int mid = node->keys.size()/2;
        for(int i = 0; i < mid; i ++) {
            newInter1.keys.push_back(node->keys[i]);
            newInter1.children.push_back(node->children[i]);
            if(i == mid-1) {
                newInter1.children.push_back(node->children[i+1]);
            }
        }
        for(int i = mid+1; i < node->keys.size(); i ++) {
            newInter2.keys.push_back(node->keys[i]);
            newInter2.children.push_back(node->children[i]);
        }

        node->keys.erase(node->keys.begin()+mid+1, node->keys.end());
        node->keys.erase(node->keys.begin(), node->keys.begin()+mid);
        node->children.clear();

        node->children.push_back(newInter1.cPage);
        node->children.push_back(newInter2.cPage);
        // TODO: consider overflow pages
        node->serializeNode(ixFileHandle);
        newInter1.serializeNode(ixFileHandle);
        newInter2.serializeNode(ixFileHandle);
    }
    return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
    if(ixFileHandle.fileHandle.getNumberOfPages() == 0) {
        return -1;
    }

    void *page = malloc(PAGE_SIZE);
    ixFileHandle.fileHandle.readPage(0, page);
    Node node = Node(&attribute, page, &ixFileHandle);
    free(page);
    node.cPage = 0;
    if(node.nodeType == RootLeaf) {
        int pos;
        bool exist;
        node.locateChildPos(pos, exist, key);
        if(!exist) {
            // -10 indicates delete a non-existing entry
            return -10;
        }

        // Locate key position, minus 1 form child position
        pos = pos-1;
        RC rc = node.deleteRecord(pos, rid);
        if(rc != 0) {
            return rc;
        }
        node.serializeNode(ixFileHandle);
        return rc;
    }
    vector<Node*> route;
    constructRouteToLeaf(ixFileHandle, route, &node, key, attribute);
    Node *leaf = route[route.size()-1];
    int pos;
    bool exist;
    leaf->locateChildPos(pos, exist, key);
    if(!exist) {
        return -10;
    }
    RC rc = leaf->deleteRecord(pos, rid);
    if(rc != 0) {
        return rc;
    }
    for(int i = 1; i < route.size(); i ++) {
        delete route[i];
    }
    route.clear();
    return rc;
}

RC IndexManager::constructRouteToLeaf(IXFileHandle &ixFileHandle, vector<Node*> &route, Node *root, const void *key, const Attribute &attribute) {
    route.push_back(root);
    if(root->nodeType == RootLeaf || root->nodeType == Leaf) {
        return 0;
    }
    int pos;
    bool exist;
    root->locateChildPos(pos, exist, key);
    int childPageNum;
    if(!exist) {
        childPageNum = root->children[pos];
    } else {
        childPageNum = root->children[pos];
    }

    void *page = (char *)malloc(PAGE_SIZE);
    ixFileHandle.fileHandle.readPage(childPageNum, page);
    Node *node = new Node(root->attribute, page, &ixFileHandle);
    node->cPage = childPageNum;
    constructRouteToLeaf(ixFileHandle, route, node, key, attribute);
    free(page);
    return 0;
}

RC IndexManager::scan(IXFileHandle &ixFileHandle,
                      const Attribute &attribute,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyKeyInclusive,
                      bool highKeyKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator) {
    if(!ixFileHandle.fileHandle.fileHandleOccupied()) {
        // Try to scan a file that doesn't exist
        return -12;
    }
    ix_ScanIterator.lowKey = lowKey;
    ix_ScanIterator.highKey = highKey;
    ix_ScanIterator.lowKeyInclusive = lowKeyKeyInclusive;
    ix_ScanIterator.highKeyInclusive = highKeyKeyInclusive;
    ix_ScanIterator.attribute = &attribute;
    ix_ScanIterator.ixFileHandle = &ixFileHandle;
    return 0;
}

void IndexManager::printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const {
    if(ixFileHandle.fileHandle.getNumberOfPages() < 1) {
        return;
    }
    int pageNum = 0;
    int indent = 0;
    printTreeElement(ixFileHandle, attribute, pageNum, 0);
}

void IndexManager::printTreeElement(IXFileHandle &ixFileHandle, const Attribute &attribute, int &pageNum, int indent) const {
    void *page = (char *)malloc(PAGE_SIZE);
    ixFileHandle.fileHandle.readPage(pageNum, page);
    Node node = Node(&attribute, page, &ixFileHandle);
    if(node.nodeType == RootLeaf || node.nodeType == Leaf) {
        node.printNodePointers(indent+1);
    } else {
        printf("%*s%s", indent, "", "{\n");
        printf("%*s%s", indent, "", "\"keys\":[");
        node.printNodeKeys();
        printf("%*s%s", indent, "", "]\n");
        printf("%*s%s", indent, "", "\"children\":[\n");
        for(int i = 0; i < node.children.size(); i ++) {
            printTreeElement(ixFileHandle, attribute, node.children[i], indent+1);
            if (i != node.children.size() - 1)
            {
                printf(",");
                printf("\n");
            }
        }
        printf("\n");
        printf("%*s%s", indent, "", "]}");
    }
    free(page);
    return;
}

RC Node::printNodeKeys() {
    for(int i = 0; i < this->keys.size(); i ++) {
        if(this->attrType == TypeInt) {
            int value;
            memcpy(&value, (char *)this->keys[i], sizeof(int));
            printf("%d", value);
        } else if(this->attrType == TypeReal) {
            float value;
            memcpy(&value, (char *)this->keys[i], sizeof(int));
            printf("%f", value);
        } else {
            int length;
            memcpy(&length, (char *)this->keys[i], sizeof(int));
            char *value = (char *)malloc(length +1);
            memcpy(value, (char *)this->keys[i]+ sizeof(int), length);
            value[length] = '\0';
            printf("%s", value);
            free(value);
        }
    }
}

RC Node::printNodePointers(int indent) {
    printf("%*s%s", indent, "", "{\n");
    printf("%*s", indent, "\"keys\": [");
    for(int i = 0; i < this->pointers.size(); i ++) {
        printf("\"");
        if (this->attrType == TypeInt)
        {
            int value;
            memcpy(&value, this->keys[i], sizeof(int));
            printf("%d:", value);
        }
        else if (this->attrType == TypeReal)
        {
            float value;
            memcpy(&value, (char *)this->keys[i], sizeof(int));
            printf("%f:", value);
        }
        else if (this->attrType == TypeVarChar)
        {
            int length;
            memcpy(&length, (char *)this->keys[i], sizeof(int));
            char* value_c = (char *)malloc(length + 1);
            memcpy(value_c, (char *)this->keys[i] + sizeof(int), length);
            value_c[length] = '\0';
            printf("%s:", value_c);
            free(value_c);
        }
        printf("[");
        for(int j = 0; j < this->pointers[i].size(); j ++) {
            printf("(%d, %d)", this->pointers[i][j].pageNum, this->pointers[i][j].slotNum);
            if (j != this->pointers[i].size() - 1)
                printf(",");
        }
        printf("]\"");
        if (i != this->keys.size() - 1)
            printf(",");
    }
    printf("]\n");
    printf("%*s%s", indent, "", "}");
    return 0;
}

IX_ScanIterator::IX_ScanIterator() {
    this->cPage = 0;
    this->curK = 0;
    this->curR = -1;
    this->prevP = -1;
    this->prevK = -1;
    this->prevR = -1;
}

IX_ScanIterator::~IX_ScanIterator() {
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
    if(this->cPage > ixFileHandle->fileHandle.getNumberOfPages()-1) {
        return -1;
    }
    if(this->node == nullptr) {
        void *page = (char *)malloc(PAGE_SIZE);
        this->ixFileHandle->fileHandle.readPage(this->cPage, page);
        this->node = new Node(this->attribute, page, this->ixFileHandle);
        this->node->cPage = this->cPage;
        free(page);
    }

    if(!(this->node->nodeType == RootLeaf || this->node->nodeType == Leaf)) {
        int leafPageNum = reachLeaf(this->node);
        this->cPage = leafPageNum;

    }
    while(1) {
        this->curR ++;
        if(this->curR >= this->node->pointers[this->curK].size() && this->node->pointers[this->curK].size() > 0) {
            this->curK ++;
            this->curR = 0;
        }
        if(this->curK >= this->node->keys.size() && this->node->next != -1) {
            this->cPage ++;
            void *page = (char *)malloc(PAGE_SIZE);
            this->ixFileHandle->fileHandle.readPage(this->cPage, page);
            delete this->node;
            this->node = new Node(attribute, page, this->ixFileHandle);
            this->node->cPage = this->cPage;
            this->curK = 0;
            this->curR = 0;
            free(page);
        }

        if(this->curK >= this->node->keys.size() && this->node->next == -1) {
            return IX_EOF;
        }
        if (this->curK >= this->node->keys.size() || this->curR >= this->node->pointers[this->curK].size()) {
            continue;
        }

        if(this->lowKey != NULL) {
            if (!((this->lowKeyInclusive && this->node->compareLarge(node->keys[this->curK], this->lowKey) >= 0) ||
                  (!this->lowKeyInclusive && this->node->compareLarge(node->keys[this->curK], this->lowKey) == 1))) {
                continue;
            }
        }
        if(this->highKey != NULL) {
            if (!((this->highKeyInclusive && this->node->compareLess(node->keys[this->curK], this->highKey) >= 0) ||
                  (!this->highKeyInclusive && this->node->compareLess(node->keys[this->curK], this->highKey) == 1))) {
                return IX_EOF;
            }
        }
        rid.pageNum = this->node->pointers[this->curK][this->curR].pageNum;
        rid.slotNum = this->node->pointers[this->curK][this->curR].slotNum;
        if (this->attribute->type == TypeInt || this->attribute->type == TypeReal) {
            memcpy(key, this->node->keys[this->curK], this->attribute->length);
        } else {
            int length;
            memcpy(&length, (char *)this->node->keys[this->curK], sizeof(int));
            memcpy(key, &length, sizeof(int));
            memcpy((char *)key + sizeof(int), (char *)this->node->keys[this->curK] + sizeof(int), length);
        }
        if(this->prevP == this->cPage) {
            void *page = malloc(PAGE_SIZE);
            ixFileHandle->fileHandle.readPage(this->cPage, page);
            delete this->node;
            this->node = new Node(attribute, page, ixFileHandle);
            this->node->cPage = this->cPage;
            RID lastRid = this->node->pointers[this->prevK][this->prevR];
            // In this situation, the prevRid has already been deleted
            if(lastRid.pageNum != this->prevRid.pageNum || lastRid.slotNum != this->prevRid.slotNum) {
                this->curK = this->prevK;
                this->curR = this->prevR;
            }
            free(page);
        }
        this->prevRid = rid;
        this->prevP = this->cPage;
        this->prevK = this->curK;
        this->prevR = this->curR;
        break;
    }
    return 0;
}

RC IX_ScanIterator::close() {
    this->cPage = 0;
    this->curK = 0;
    this->curR = -1;
    if(this->node != NULL) {
        delete this->node;
        this->node = NULL;
    }
    return 0;
}

int IX_ScanIterator::reachLeaf(Node *&node) {
    int pageNum = 0;
    int pos;
    bool exist;
    while(!(node->nodeType == RootLeaf || node->nodeType == Leaf)) {
        if(!this->lowKey == NULL) {
            node->locateChildPos(pos, exist, this->lowKey);
        } else {
            pos = 0;
        }
        pageNum = node->children[pos];
        void *page = (char *)malloc(PAGE_SIZE);
        this->ixFileHandle->fileHandle.readPage(pageNum, page);
        delete node;
        node = new Node(this->attribute, page, ixFileHandle);
        node->cPage = pageNum;
        free(page);
    }
    return pageNum;
}

IXFileHandle::IXFileHandle() {
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle() {
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    fileHandle.collectCounterValues(this->ixReadPageCounter, this->ixWritePageCounter, this->ixAppendPageCounter);
    readPageCount = this->ixReadPageCounter;
    writePageCount = this->ixWritePageCounter;
    appendPageCount = this->ixAppendPageCounter;
    return 0;
}

Node::~Node() {
    for (int i = 0; i < this->keys.size(); ++i)
    {
        free(keys[i]);
    }
}

Node::Node(const Attribute &attribute) {
    this->attrType = attribute.type;
    this->attribute = &attribute;
}

Node::Node(const Attribute *attribute) {
    this->attrType = attribute->type;
    this->attribute = attribute;
}

Node::Node(const Attribute *attribute, const void *page, IXFileHandle *ixfileHandle) {
    this->attrType = attribute->type;
    this->attribute = attribute;
    int offset = 0;
    memcpy(&this->nodeType, (char *)page+offset, sizeof(int));
    offset += sizeof(int);
    memcpy(&this->previous, (char *)page+offset, sizeof(int));
    offset += sizeof(int);
    memcpy(&this->next, (char *)page+offset, sizeof(int));
    offset += sizeof(int);

    // Copy keys to Node
    int nKeys;
    memcpy(&nKeys, (char *)page+offset, sizeof(int));
    offset += sizeof(int);
    for(int i = 0; i < nKeys; i ++) {
        if(this->attrType == TypeInt || this->attrType == TypeReal) {
            void *value = malloc(attribute->length);
            memcpy(value, (char *)page+offset, sizeof(int));
            offset += attribute->length;
            this->keys.push_back(value);
        } else if(this->attrType == TypeVarChar) {
            int length;
            memcpy(&length, (char *)page+offset, sizeof(int));
            offset += sizeof(int);
            void *value = malloc(length + sizeof(int));
            memcpy(value, &length, sizeof(int));
            memcpy((char *)value+sizeof(int), (char *)page+offset, length);
            offset += length;
            this->keys.push_back(value);
        }
    }

    // Copy rids to Node
    if(this->nodeType == Leaf || this->nodeType == RootLeaf) {
        this->cPage = 0;
        int nRids;
        memcpy(&nRids, (char *)page+offset, sizeof(int));
        offset += sizeof(int);
        for(int i = 0; i < nRids; i ++) {
            vector<RID> rids;
            RID rid;
            int nRid;
            memcpy(&nRid, (char *)page+offset, sizeof(int));
            offset += sizeof(int);
            for(int j = 0; j < nRid; j ++) {
                memcpy(&rid.pageNum, (char *)page+offset, sizeof(int));
                offset += sizeof(int);
                memcpy(&rid.slotNum, (char *)page+offset, sizeof(int));
                offset += sizeof(int);
                rids.push_back(rid);
            }
            this->pointers.push_back(rids);
        }
    } else {
        int nChildren;
        memcpy(&nChildren, (char *)page+offset, sizeof(int));
        offset += sizeof(int);
        for(int i = 0; i < nChildren; i ++) {
            int childPageNum;
            memcpy(&childPageNum, (char *)page+offset, sizeof(int));
            offset += sizeof(int);
            this->children.push_back(childPageNum);
        }
    }

    int overFlowPage;
    memcpy(&overFlowPage, (char *)page + offset, sizeof(int));
    offset += sizeof(int);
    if (this->nodeType == RootLeaf || this->nodeType == Root)
        this->cPage = 0;
    if (overFlowPage != -1)
    {
        this->isOverflow = true;
        this->overFlowPages.push_back(overFlowPage);
        this->deserializeOverflowPage(overFlowPage, ixfileHandle);
    }
}

RC Node::deserializeOverflowPage(int nodeId, IXFileHandle *ixfileHandle) {
    void *page = malloc(PAGE_SIZE);
    ixfileHandle->fileHandle.readPage(nodeId, page);
    int offset = 0;
    int nRids;
    memcpy(&nRids, (char *)page + offset, sizeof(int));
    offset += sizeof(int);
    for (int i = 0; i < nRids; ++i)
    {
        RID rid;
        memcpy(&rid.pageNum, (char *)page + offset, sizeof(int));
        offset += sizeof(int);
        memcpy(&rid.slotNum, (char *)page + offset, sizeof(int));
        offset += sizeof(int);
        this->pointers[0].push_back(rid);
    }
    int overFlowPage;
    memcpy(&overFlowPage, (char *)page + offset, sizeof(int));
    offset += sizeof(int);
    if (overFlowPage != -1)
    {
        this->overFlowPages.push_back(overFlowPage);
        this->deserializeOverflowPage(overFlowPage, ixfileHandle);
    }
    free(page);
    return 0;
}

RC Node::serializeNode(IXFileHandle &ixfileHandle) {
    if((this->nodeType == RootLeaf || this->nodeType == Leaf) && this->keys.size() == 1 && this->getNodeSize() > PAGE_SIZE) {
        int nRidInOP = (PAGE_SIZE - 2* sizeof(int))/(2* sizeof(int));
        int nRidInNode = (PAGE_SIZE - this->calHeaderSize())/(2* sizeof(int));
        int left = this->pointers[0].size() - nRidInNode;
        int nNewRidToWrite = left;
        if(this->overFlowPages.size() > 0) {
            for(int i = 0; i < this->overFlowPages.size(); i ++) {
                nNewRidToWrite -= nRidInOP;
            }
        }
        if(nNewRidToWrite > 0) {
            int nNeededPage = nNewRidToWrite/nRidInOP + 1;
            for(int i = 0; i < nNeededPage; i ++) {
                void *page = (char *)malloc(PAGE_SIZE);
                ixfileHandle.fileHandle.appendPage(page);
                this->overFlowPages.push_back(ixfileHandle.fileHandle.getNumberOfPages()-1);
                free(page);
            }
        }

        int offset = 0;
        void *page = (char *)malloc(PAGE_SIZE);
        memcpy((char *)page+offset, &this->nodeType, sizeof(int));
        offset += sizeof(int);
        memcpy((char *)page+offset, &this->previous, sizeof(int));
        offset += sizeof(int);
        memcpy((char *)page+offset, &this->next, sizeof(int));
        offset += sizeof(int);
        int nKeys = this->keys.size();
        memcpy((char *)page+offset, &nKeys, sizeof(int));
        offset += sizeof(int);
        for(int i = 0; i < nKeys; i ++) {
            memcpy((char *)page+offset, this->keys[i], sizeof(int));
            offset += sizeof(int);
        }
        int nRids = this->pointers.size();
        memcpy((char *)page+offset, &nRids, sizeof(int));
        offset += sizeof(int);
        for(int i = 0; i < this->pointers.size(); i ++) {
            memcpy((char *)page+offset, &nRidInNode, sizeof(int));
            offset += sizeof(int);
            for(int j = 0; j < nRidInNode; j ++) {
                int pNum = this->pointers[i][j].pageNum;
                int sNum = this->pointers[i][j].slotNum;
                memcpy((char *)page+offset, &pNum, sizeof(int));
                offset += sizeof(int);
                memcpy((char *)page+offset, &sNum, sizeof(int));
                offset += sizeof(int);
            }
        }

        int firstOP = this->overFlowPages[0];
        memcpy((char *)page+offset, &firstOP, sizeof(int));
        offset += sizeof(int);
        ixfileHandle.fileHandle.writePage(this->cPage, page);

        void *overflowPage = (char *)malloc(PAGE_SIZE);
        for(int i = 0; i < this->overFlowPages.size(); i ++) {
            if(i < this->overFlowPages.size()-1) {
                int ridsSize = this->serializeOverflowPage(i*nRidInOP+nRidInNode, (i+1)*nRidInOP+nRidInNode, overflowPage);
                memcpy((char *)overflowPage+ridsSize, &this->overFlowPages[i+1], sizeof(int));
            } else {
                int ridsSize = this->serializeOverflowPage(i*nRidInOP+nRidInNode, this->pointers[0].size(), overflowPage);
                int nextPage = -1;
                memcpy((char *)overflowPage+ridsSize, &nextPage, sizeof(int));
            }
            ixfileHandle.fileHandle.writePage(this->overFlowPages[i], overflowPage);
        }
        free(overflowPage);
        return 0;
    } else {
        void *page = (char *)malloc(PAGE_SIZE);
        this->serialize(page);
        ixfileHandle.fileHandle.writePage(this->cPage, page);
        free(page);
    }

    return 0;
}

RC Node::serialize(void *page) {
    int offset = 0;
    memcpy((char *)page+offset, &this->nodeType, sizeof(int));
    offset += sizeof(NodeType);
    memcpy((char *)page+offset, &this->previous, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)page+offset, &this->next, sizeof(int));
    offset += sizeof(int);

    int nKeys = this->keys.size();
    memcpy((char *)page+offset, &nKeys, sizeof(int));
    offset += sizeof(int);
    for(int i = 0; i < nKeys; i ++) {
        if(this->attrType == TypeInt || this->attrType == TypeReal) {
            memcpy((char *)page+offset, this->keys[i], sizeof(int));
            offset += sizeof(int);
        } else if(this->attrType == TypeVarChar) {
            int length;
            memcpy(&length, this->keys[i], sizeof(int));
            memcpy((char *)page+offset, this->keys[i], sizeof(int)+length);
            memcpy(&length, (char *)page+offset, sizeof(int));
            offset += (sizeof(int) + length);
        }
    }

    if(this->nodeType == Leaf || this->nodeType == RootLeaf) {
        int nRids = this->pointers.size();
        memcpy((char *)page+offset, &nRids, sizeof(int));
        offset += sizeof(int);
        for(int i = 0; i < nRids; i ++) {
            int nRid = this->pointers[i].size();
            memcpy((char *)page+offset, &nRid, sizeof(int));
            offset += sizeof(int);
            for(int j = 0; j < nRid; j ++) {
                int pNum = this->pointers[i][j].pageNum;
                int sNum = this->pointers[i][j].slotNum;
                memcpy((char *)page+offset, &pNum, sizeof(int));
                offset += sizeof(int);
                memcpy((char *)page+offset, &sNum, sizeof(int));
                offset += sizeof(int);
            }
        }
    } else {
        int nChildren = this->children.size();
        memcpy((char *)page+offset, &nChildren, sizeof(int));
        offset += sizeof(int);
        for(int i = 0; i < nChildren; i ++) {
            int childPageNum = this->children[i];
            memcpy((char *)page+offset, &childPageNum, sizeof(int));
            offset += sizeof(int);
        }
    }

    int overFlowPage = -1;
    memcpy((char *)page + offset, &overFlowPage, sizeof(int));
    offset += sizeof(int);
    return 0;
}

int Node::serializeOverflowPage(int start, int end, void *page) {
    int nRid = end - start;
    int offset = 0;
    memcpy((char *)page+offset, &nRid, sizeof(int));
    offset += sizeof(int);
    for(int i = start; i < end; i ++) {
        int pNum = this->pointers[0][i].pageNum;
        int sNum = this->pointers[0][i].slotNum;
        memcpy((char *)page + offset, &pNum, sizeof(int));
        offset += sizeof(int);
        memcpy((char *)page + offset, &sNum, sizeof(int));
        offset += sizeof(int);
    }
    return offset;
}

RC Node::insertKey(int &pos, const void *key) {
    void *value;
    if(this->attrType == TypeInt || this->attrType == TypeReal) {
        value = malloc(attribute->length);
        memcpy(value, key, sizeof(int));
    } else {
        int length;
        memcpy(&length, key, sizeof(int));
        value = malloc(length + sizeof(int));
        memcpy(value, &length, sizeof(int));
        memcpy((char *)value+sizeof(int), (char *)key+ sizeof(int), length);
    }

    if(this->keys.size() >= 1) {
        this->keys.insert(this->keys.begin()+pos, (void *)value);
    } else {
        this->keys.push_back((void *)value);
    }
}

RC Node::insertPointer(int pos, const bool &exist, const RID &rid) {
    if(!exist && this->pointers.size() >= 1) {
        vector<RID> rids;
        rids.push_back(rid);
        this->pointers.insert(this->pointers.begin()+pos, rids);
    } else if(!exist && this->pointers.size() == 0) {
        vector<RID> rids;
        rids.push_back(rid);
        this->pointers.push_back(rids);
    }
    else {
        this->pointers[pos].push_back(rid);
    }
}

RC Node::insertChild(const int &pos, int &pageNum) {
    this->children.insert(this->children.begin()+pos, pageNum);
    return 0;
}

RC Node::deleteRecord(int pos, const RID &rid) {
    for(int j = 0; j < this->pointers[pos].size(); j ++) {
        if(this->pointers[pos].size() == 1) {
            this->keys.erase(this->keys.begin()+pos, this->keys.begin()+pos+1);
            return 0;
        }
        if(rid.pageNum == this->pointers[pos][j].pageNum && rid.slotNum == this->pointers[pos][j].slotNum) {
            this->pointers[pos].erase(this->pointers[pos].begin()+j, this->pointers[pos].begin()+j+1);
            return 0;
        }
    }
    // Delete an rid that doesn't exist
    return -11;
}

RC Node::locateChildPos(int &pos, bool &exist, const void *value) {
    int i;
    for(i = 0; i < this->keys.size(); i ++) {
        if(compareEqual(value, keys[i])) {
            pos = i+1;
            exist = true;
            return 0;
        }
        if(compareLess(value, keys[i]) == 1) {
            pos = i;
            exist = false;
            return 0;
        }
    }
    pos = i;
    exist = false;
    return 0;
}

bool Node::compareEqual(const void *compValue, const void *compKey) {
    bool equal;
    switch (this->attrType) {
        case TypeInt:
            int valueI;
            int keyI;
            memcpy(&valueI, compValue, sizeof(int));
            memcpy(&keyI, compKey, sizeof(int));
            equal = valueI == keyI;
            break;
        case TypeReal:
            float valueII;
            float keyII;
            memcpy(&valueII, compValue, sizeof(int));
            memcpy(&keyII, compKey, sizeof(int));
            equal = valueII == keyII;
            break;
        case TypeVarChar:
            string valueStr;
            string keyStr;
            int valueLen;
            int keyLen;
            memcpy(&valueLen, (char *)compValue, sizeof(int));
            memcpy(&keyLen, (char *)compKey, sizeof(int));
            char *value = (char *)malloc(valueLen+1);
            char *key = (char *)malloc(keyLen+1);
            memcpy(value, (char *)compValue+ sizeof(int), valueLen);
            memcpy(key, (char *)compKey+ sizeof(int), keyLen);
            value[valueLen] = '\0';
            key[keyLen] = '\0';
            valueStr = string(value);
            keyStr = string(key);
            equal = valueStr == keyStr;
            break;
    }
    return equal;
}

int Node::compareLess(const void *compValue, const void *compKey) {
    if(this->attrType == TypeInt) {
        int value;
        int key;
        memcpy(&value, compValue, sizeof(int));
        memcpy(&key, compKey, sizeof(int));
        if(value < key) {
            return 1;
        } else if(value == key){
            return 0;
        }
    } else if(this->attrType == TypeReal) {
        float value;
        float key;
        memcpy(&value, compValue, sizeof(int));
        memcpy(&key, compKey, sizeof(int));
        if(value < key) {
            return 1;
        } else if(value == key){
            return 0;
        }
    } else if(this->attrType == TypeVarChar) {
        string valueStr;
        string keyStr;
        int valueLen;
        int keyLen;
        memcpy(&valueLen, (char *)compValue, sizeof(int));
        memcpy(&keyLen, (char *)compKey, sizeof(int));
        char *value = (char *)malloc(valueLen+1);
        char *key = (char *)malloc(keyLen+1);
        memcpy(value, (char *)compValue+sizeof(int), valueLen);
        memcpy(key, (char *)compKey+ sizeof(int), keyLen);
        value[valueLen] = '\0';
        key[keyLen] = '\0';
        valueStr = string(value);
        keyStr = string(key);
        if(valueStr < keyStr) {
            return 1;
        } else if(valueStr == keyStr){
            return 0;
        }
    }
    return -1;
}

int Node::compareLarge(const void *compValue, const void *compKey) {
    if(this->attrType == TypeInt) {
        int value;
        int key;
        memcpy(&value, compValue, sizeof(int));
        memcpy(&key, compKey, sizeof(int));
        if(value > key) {
            return 1;
        } else if(value == key){
            return 0;
        }
    } else if(this->attrType == TypeReal) {
        float value;
        float key;
        memcpy(&value, compValue, sizeof(int));
        memcpy(&key, compKey, sizeof(int));
        if(value > key) {
            return 1;
        } else if(value == key){
            return 0;
        }
    } else if(this->attrType == TypeVarChar) {
        string valueStr;
        string keyStr;
        int valueLen;
        int keyLen;
        memcpy(&valueLen, (char *)compValue, sizeof(int));
        memcpy(&keyLen, (char *)compKey, sizeof(int));
        char *value = (char *)malloc(valueLen+1);
        char *key = (char *)malloc(keyLen+1);
        memcpy(value, (char *)compValue+sizeof(int), valueLen);
        memcpy(key, (char *)compKey+ sizeof(int), keyLen);
        value[valueLen] = '\0';
        key[keyLen] = '\0';
        valueStr = string(value);
        keyStr = string(key);
        if(valueStr > keyStr) {
            return 1;
        } else if(valueStr == keyStr){
            return 0;
        }
    }
    return -1;
}

int Node::getNodeSize() {
    int size = 0;
    size += sizeof(this->nodeType);
    size += 3 * sizeof(int);
    for(int i = 0; i < this->keys.size(); i ++) {
        if(this->attrType == TypeInt || this->attrType == TypeReal) {
            size += sizeof(int);
        } else {
            int length;
            memcpy(&length, (char *)this->keys[i], sizeof(int));
            size += sizeof(int) + length;
        }
    }

    if(this->nodeType == Leaf || this->nodeType == RootLeaf) {
        // # of rids
        size += sizeof(int);
        for(int i = 0; i < this->pointers.size(); i ++) {
            // # of rid
            size += sizeof(int);
            for(int j = 0; j < this->pointers[i].size(); j ++) {
                size += 2 * sizeof(int);
            }
        }
    } else {
        // # of children
        size += sizeof(int);
        for(int i = 0; i < this->children.size(); i ++) {
            size += sizeof(int);
        }
    }
    // isOverflow
    size += sizeof(int);
    this->size = size;
    return size;
}

int Node::calHeaderSize() {
    int size = 0;
    size += 3 * sizeof(int);
    size += sizeof(int);
    int nKeys = this->keys.size();
    for(int i = 0; i < nKeys; i ++) {
        if(this->attrType == TypeInt || this->attrType == TypeReal) {
            size += sizeof(int);
        } else {
            int length;
            memcpy(&length, (char *)this->keys[i], sizeof(int));
            size += (sizeof(int) + length);
        }
    }
    size += 2*sizeof(int);
    return size;
}