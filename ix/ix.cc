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
    AttrValue attrValue;
    attrValue.readAttr(attribute.type, key);
    void *page = malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);
    if (ixFileHandle.fileHandle.getNumberOfPages() == 0) {
        Node root = Node(attribute.type);
        root.nodeType = SingleRoot;
        root.keys.emplace_back(attrValue);
        vector<RID> rids;
        rids.emplace_back(rid);
        root.pointers.emplace_back(rids);
        root.pageNum = 0;
        root.serialize(page);
        ixFileHandle.fileHandle.appendPage(page);
    } else {
        ixFileHandle.fileHandle.readPage(0, page);
        Node root = Node(ixFileHandle, attribute.type, page);
        vector<Node *> route;
        if (root.nodeType == SingleRoot) {
            int pos = root.locateChildPos(attrValue, LT_OP);
            root.insertKey(pos, attrValue);
            root.insertPointer(pos, attrValue, rid);
            route.emplace_back(&root);

            if (!root.keys.empty() && root.getNodeSize() > PAGE_SIZE) {
                split(ixFileHandle, route);
            } else {
                root.writeNode(ixFileHandle);
            }
        } else {
            routeToLeaf(ixFileHandle, route, &root, attrValue);
            Node *leaf = route[route.size() - 1];
            int pos = leaf->locateChildPos(attrValue, LT_OP);
            leaf->insertKey(pos, attrValue);
            leaf->insertPointer(pos, attrValue, rid);
            if (!leaf->keys.empty() && leaf->getNodeSize() > PAGE_SIZE) {
                split(ixFileHandle, route);
            } else {
                leaf->writeNode(ixFileHandle);
            }
        }
        for (int i = 1; i < route.size(); i++) {
            delete route[i];
        }
        route.clear();
    }
    free(page);
    return 0;
}

RC IndexManager::split(IXFileHandle &ixFileHandle, vector<Node *> &route) {
    Node *node = route[route.size() - 1];
    void *emptyPage = malloc(PAGE_SIZE);
    memset(emptyPage, 0, PAGE_SIZE);
    if (node->nodeType == Leaf) {
        Node newLeaf = Node(node->attrType);
        newLeaf.nodeType = Leaf;
        newLeaf.overFlowPages = node->overFlowPages;

        int mid = node->keys.size() / 2;
        for (int i = mid; i < node->keys.size(); i++) {
            newLeaf.keys.emplace_back(node->keys[i]);
            newLeaf.pointers.emplace_back(node->pointers[i]);
        }
        node->keys.erase(node->keys.begin() + mid, node->keys.begin() + node->keys.size());
        node->pointers.erase(node->pointers.begin() + mid, node->pointers.begin() + node->pointers.size());

        ixFileHandle.fileHandle.appendPage(emptyPage);
        newLeaf.pageNum = ixFileHandle.fileHandle.getNumberOfPages() - 1;
        newLeaf.next = node->next;
        node->next = newLeaf.pageNum;
        newLeaf.previous = node->pageNum;
        node->writeNode(ixFileHandle);
        newLeaf.writeNode(ixFileHandle);

        Node *parent = route[route.size() - 2];
        int pos = parent->locateChildPos(newLeaf.keys[0], LT_OP);
        parent->insertKey(pos, newLeaf.keys[0]);
        parent->insertChild(pos + 1, newLeaf.pageNum);
        // Delete the top element
        delete route[route.size() - 1];
        route.pop_back();

        if (!parent->keys.empty() && parent->getNodeSize() > PAGE_SIZE) {
            split(ixFileHandle, route);
        } else {
            parent->writeNode(ixFileHandle);
        }
    } else if (node->nodeType == Intermediate) {
        Node newInter = Node(node->attrType);
        newInter.nodeType = node->nodeType;
        // Strictly less than when comes to intermediate node
        int mid = node->keys.size() / 2;
        for (int i = mid + 1; i < node->keys.size(); i++) {
            newInter.keys.emplace_back(node->keys[i]);
            newInter.children.emplace_back(node->children[i]);
        }
        newInter.children.emplace_back(node->children[node->keys.size()]);

        ixFileHandle.fileHandle.appendPage(emptyPage);
        newInter.pageNum = ixFileHandle.fileHandle.getNumberOfPages() - 1;
        Node *parent = route[route.size() - 2];
        int pos = parent->locateChildPos(node->keys[mid], LT_OP);
        parent->insertKey(pos, node->keys[mid]);
        parent->insertChild(pos + 1, newInter.pageNum);

        newInter.writeNode(ixFileHandle);
        node->keys.erase(node->keys.begin() + mid, node->keys.begin() + node->keys.size());
        node->children.erase(node->children.begin() + mid + 1, node->children.begin() + node->children.size());
        node->writeNode(ixFileHandle);

        delete route[route.size() - 1];
        route.pop_back();
        if (!parent->keys.empty() && parent->getNodeSize() > PAGE_SIZE) {
            split(ixFileHandle, route);
        } else {
            parent->writeNode(ixFileHandle);
        }
    } else if (node->nodeType == SingleRoot) {
        Node newLeaf1 = Node(node->attrType);
        Node newLeaf2 = Node(node->attrType);
        node->nodeType = Root;
        newLeaf1.nodeType = Leaf;
        newLeaf2.nodeType = Leaf;
        newLeaf1.overFlowPages = node->overFlowPages;
        newLeaf2.overFlowPages = node->overFlowPages;
        int mid = node->keys.size() / 2;
        for (int i = 0; i < node->keys.size(); i++) {
            if (i < mid) {
                newLeaf1.keys.emplace_back(node->keys[i]);
                newLeaf1.pointers.emplace_back(node->pointers[i]);
            } else {
                newLeaf2.keys.emplace_back(node->keys[i]);
                newLeaf2.pointers.emplace_back(node->pointers[i]);
            }
        }
        node->keys.erase(node->keys.begin() + mid + 1, node->keys.begin() + node->keys.size());
        node->keys.erase(node->keys.begin(), node->keys.begin() + mid);
        node->pointers.clear();

        ixFileHandle.fileHandle.appendPage(emptyPage);
        newLeaf1.pageNum = ixFileHandle.fileHandle.getNumberOfPages() - 1;
        ixFileHandle.fileHandle.appendPage(emptyPage);
        newLeaf2.pageNum = ixFileHandle.fileHandle.getNumberOfPages() - 1;
        node->children.emplace_back(newLeaf1.pageNum);
        node->children.emplace_back(newLeaf2.pageNum);
        node->writeNode(ixFileHandle);
        newLeaf1.next = newLeaf2.pageNum;
        newLeaf2.previous = newLeaf1.pageNum;
        newLeaf1.writeNode(ixFileHandle);
        newLeaf2.writeNode(ixFileHandle);
    } else if (node->nodeType == Root) {
        Node newInter1 = Node(node->attrType);
        Node newInter2 = Node(node->attrType);
        newInter1.nodeType = Intermediate;
        newInter2.nodeType = Intermediate;
        int mid = node->keys.size() / 2;
        // TODO: Index leaves out mid key
        for (int i = 0; i < node->keys.size(); i++) {
            if (i < mid) {
                newInter1.keys.emplace_back(node->keys[i]);
            } else if (i > mid) {
                newInter2.keys.emplace_back(node->keys[i]);
            }
        }
        for (int i = 0; i < node->children.size(); i++) {
            if (i < mid + 1) {
                newInter1.children.emplace_back(node->children[i]);
            } else {
                newInter2.children.emplace_back(node->children[i]);
            }
        }

        node->keys.erase(node->keys.begin() + mid + 1, node->keys.begin() + node->keys.size());
        node->keys.erase(node->keys.begin(), node->keys.begin() + mid);
        node->children.clear();

        ixFileHandle.fileHandle.appendPage(emptyPage);
        newInter1.pageNum = ixFileHandle.fileHandle.getNumberOfPages() - 1;
        ixFileHandle.fileHandle.appendPage(emptyPage);
        newInter2.pageNum = ixFileHandle.fileHandle.getNumberOfPages() - 1;
        node->children.clear();
        node->children.emplace_back(newInter1.pageNum);
        node->children.emplace_back(newInter2.pageNum);

        node->writeNode(ixFileHandle);
        newInter1.writeNode(ixFileHandle);
        newInter2.writeNode(ixFileHandle);
    }
    free(emptyPage);
    return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
    if (ixFileHandle.fileHandle.getNumberOfPages() == 0) {
        return -1;
    }

    AttrValue attrValue;
    attrValue.readAttr(attribute.type, key);
    void *page = malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);
    ixFileHandle.fileHandle.readPage(0, page);
    Node node = Node(ixFileHandle, attribute.type, page);
    free(page);
    node.pageNum = 0;
    if (node.nodeType == SingleRoot) {
        int pos = node.locateChildPos(attrValue, EQ_OP);
        if (pos < 0) {
            // -10 indicates delete a non-existing entry
            return -10;
        }

        RC rc = node.deleteRecord(pos, rid);
        if (rc != 0) {
            return rc;
        }
        node.writeNode(ixFileHandle);
        return rc;
    }
    vector<Node *> route;
    routeToLeaf(ixFileHandle, route, &node, attrValue);
    Node *leaf = route[route.size() - 1];
    int pos = leaf->locateChildPos(attrValue, EQ_OP);
    if (pos < 0) {
        return -10;
    }
    RC rc = leaf->deleteRecord(pos, rid);
    if (rc != 0) {
        return rc;
    }
    this->checkMerge(ixFileHandle, route);
    for (int i = 1; i < route.size(); i++) {
        delete route[i];
    }
    route.clear();
    return rc;
}

RC IndexManager::checkMerge(IXFileHandle &ixFileHandle, vector<Node *> &route) {
    Node *node = route[route.size() - 1];
    Node *parent = route[route.size() - 2];

    if (parent->children.size() < 2) {
        return -1;
    }
    int sibPageNum = -1;
    int pos = -1;
    int siblingType = 0;

    if (parent->children[parent->children.size() - 1] != node->pageNum) {
        sibPageNum = node->getRightSibling(parent, pos);
        siblingType = 1; // denote right sibling
    } else if (parent->children[0] != node->pageNum) {
        sibPageNum = node->getLeftSibling(parent, pos);
        siblingType = -1; // denote left sibling
    }
    if (sibPageNum == -1 && pos == -1) {
        return -1;
    }
    void *page = malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);
    ixFileHandle.fileHandle.readPage(sibPageNum, page);
    Node *sibNode = new Node(ixFileHandle, node->attrType, page);
    free(page);
    sibNode->pageNum = sibPageNum;
    int nodeSize = node->getNodeSize();
    int sibSize = sibNode->getNodeSize();
    if (nodeSize < PAGE_SIZE / 2 && sibSize < PAGE_SIZE / 2 && nodeSize + sibSize + sizeof(int) < PAGE_SIZE) {
        this->merge(ixFileHandle, sibNode, route, pos, siblingType);
    } else if (nodeSize < PAGE_SIZE / 2) {
        this->borrow(ixFileHandle, node, sibNode, parent, pos, siblingType);
    } else {
        node->writeNode(ixFileHandle);
    }
    return 0;
}

RC IndexManager::merge(IXFileHandle &ixFileHandle, Node *sibNode, vector<Node *> &route, int &pos, int &siblingType) {
    Node *node = route[route.size() - 1];
    Node *parent = route[route.size() - 2];
    if (siblingType == 1) {
        if (node->nodeType == Leaf) {
            if (parent->nodeType == Root && parent->keys.size() == 1) {
                node->nodeType = SingleRoot;
                node->pageNum = 0;
                parent->pageNum = -1;
            }
            for (int i = 0; i < sibNode->keys.size(); i++) {
                node->keys.emplace_back(sibNode->keys[i]);
                node->pointers.emplace_back(sibNode->pointers[i]);
            }
            parent->keys.erase(parent->keys.begin() + pos);
            //TODO: check children erase here
            parent->children.erase(parent->children.begin() + pos + 1);
            node->next = sibNode->next;
            if (node->nodeType == Leaf && sibNode->next != -1) {
                void *page = malloc(PAGE_SIZE);
                memset(page, 0, PAGE_SIZE);
                ixFileHandle.fileHandle.readPage(node->next, page);
                Node *sibNextNode = new Node(ixFileHandle, node->attrType, page);
                free(page);
                sibNextNode->previous = node->pageNum;
                sibNextNode->writeNode(ixFileHandle);
            }
        } else if (node->nodeType == Intermediate) {
            if (parent->nodeType == Root && parent->keys.size() == 1) {
                node->nodeType = Root;
                node->pageNum = 0;
                parent->pageNum = -1;
            }
            node->keys.emplace_back(parent->keys[pos]);
            for (int i = 0; i < sibNode->keys.size(); i++) {
                node->keys.emplace_back(sibNode->keys[i]);
                node->children.emplace_back(sibNode->children[i]);
            }
            node->children.emplace_back(sibNode->children[sibNode->keys.size()]);
            parent->keys.erase(parent->keys.begin() + pos);
            parent->children.erase(parent->children.begin() + pos + 1);
        }
        parent->writeNode(ixFileHandle);
        node->writeNode(ixFileHandle);
    } else if (siblingType == -1) {
        pos--;
        if (node->nodeType == Leaf) {
            if (parent->nodeType == Root && parent->keys.size() == 1) {
                sibNode->nodeType = SingleRoot;
                sibNode->pageNum = 0;
                parent->pageNum = -1;
            }
            for (int i = 0; i < node->keys.size(); i++) {
                sibNode->keys.emplace_back(node->keys[i]);
                sibNode->pointers.emplace_back(node->pointers[i]);
            }
            parent->keys.erase(parent->keys.begin() + pos);
            parent->children.erase(parent->children.begin() + pos + 1);
            //TODO: nodType check for sibNode
            if (node->nodeType == Leaf) {
                sibNode->next = node->next;
            }
        } else if (node->nodeType == Intermediate) {
            if (parent->nodeType == Root && parent->keys.size() == 1) {
                sibNode->nodeType = Root;
                sibNode->pageNum = 0;
                parent->pageNum = -1;
            }
            sibNode->keys.emplace_back(parent->keys[pos]);
            for (int i = 0; i < node->keys.size(); i++) {
                sibNode->keys.emplace_back(node->keys[i]);
                sibNode->children.emplace_back(node->children[i]);
            }
            sibNode->children.emplace_back(node->children[node->keys.size()]);
            parent->keys.erase(parent->keys.begin() + pos);
            parent->children.erase(parent->children.begin() + pos + 1);
        }
        parent->writeNode(ixFileHandle);
        sibNode->writeNode(ixFileHandle);
    }
    delete route[route.size() - 1];
    route.pop_back();
    if (route.size() == 1) {
        // parent is root, no merge to check
        return 0;
    }

    Node *grandParent = route[route.size() - 2];

    int uncPageNum = -1;
    int position = -1;
    if (grandParent->children[grandParent->children.size() - 1] != parent->pageNum) {
        uncPageNum = parent->getRightSibling(grandParent, position);
        siblingType = 1;
    } else if (grandParent->children[0] != parent->pageNum) {
        uncPageNum = parent->getLeftSibling(grandParent, position);
        siblingType = -1;
    }
    if (uncPageNum == -1 && position == -1) {
        return -1;
    }
    void *page = malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);
    ixFileHandle.fileHandle.readPage(uncPageNum, page);
    Node *uncNode = new Node(ixFileHandle, parent->attrType, page);
    free(page);
    uncNode->pageNum = uncPageNum;
    int parentSize = parent->getNodeSize();
    int uncleSize = uncNode->getNodeSize();
    if (parentSize < PAGE_SIZE / 2 && uncleSize < PAGE_SIZE / 2 &&
        parentSize + uncleSize + sizeof(int) < PAGE_SIZE) {
        this->merge(ixFileHandle, uncNode, route, position, siblingType);
    } else if (parentSize < PAGE_SIZE / 2) {
        this->borrow(ixFileHandle, parent, uncNode, grandParent, position, siblingType);
    } else {
        return 0;
    }
}

RC IndexManager::borrow(IXFileHandle &ixFileHandle, Node *node, Node *sibNode, Node *parent, int &pos,
                        int &siblingType) {
    if (sibNode->keys.size() <= 1) {
        return -1;
    }
    if (siblingType == 0) {
        if (node->nodeType == Leaf) {
            if (node->getNodeSize() + sibNode->keys[0].length +
                sibNode->pointers[0].size() * sizeof(RID) > PAGE_SIZE) {
                return -1;
            }
            node->keys.emplace_back(sibNode->keys[0]);
            node->pointers.emplace_back(sibNode->pointers[0]);
            sibNode->keys.erase(sibNode->keys.begin());
            sibNode->pointers.erase(sibNode->pointers.begin());
            parent->keys[pos] = sibNode->keys[0];
        } else if (node->nodeType == Intermediate) {
            if (node->getNodeSize() + sibNode->keys[0].length + sizeof(int) > PAGE_SIZE) {
                return -1;
            }
            node->keys.emplace_back(parent->keys[pos]);
            node->children.emplace_back(sibNode->children[0]);
            parent->keys[pos] = sibNode->keys[0];
            sibNode->keys.erase(sibNode->keys.begin());
            sibNode->children.erase(sibNode->children.begin());
        }
    } else if (siblingType == -1) {
        int kSize = sibNode->keys.size();
        if (node->nodeType == Leaf) {
            int ptSize = sibNode->pointers.size();
            if (node->getNodeSize() + sibNode->keys[kSize - 1].length +
                sibNode->pointers[ptSize - 1].size() * sizeof(RID) > PAGE_SIZE) {
                return -1;
            }
            node->insertKey(0, sibNode->keys[kSize - 1]);
            node->pointers.insert(node->pointers.begin(), sibNode->pointers[ptSize - 1]);
            sibNode->keys.erase(sibNode->keys.begin() + kSize - 1);
            sibNode->pointers.erase(sibNode->pointers.begin() + ptSize - 1);
            parent->keys[pos - 1] = sibNode->keys[0];
        } else if (node->nodeType == Intermediate) {
            int chSize = sibNode->children.size();
            if (node->getNodeSize() + sibNode->keys[kSize - 1].length + sizeof(int) > PAGE_SIZE) {
                return -1;
            }
            node->insertKey(0, parent->keys[pos - 1]);
            node->insertChild(0, sibNode->children[chSize - 1]);
            parent->keys[pos - 1] = sibNode->keys[kSize - 1];

            sibNode->keys.erase(sibNode->keys.begin() + kSize - 1);
            sibNode->children.erase(sibNode->children.begin() + chSize - 1);
        }
    }
    node->writeNode(ixFileHandle);
    sibNode->writeNode(ixFileHandle);
    parent->writeNode(ixFileHandle);
    return 0;
}

RC IndexManager::routeToLeaf(IXFileHandle &ixFileHandle, vector<Node *> &route,
                             Node *root, AttrValue &attrValue) {
    route.emplace_back(root);
    if (root->nodeType == Leaf) {
        return 0;
    }
    int pos = root->locateChildPos(attrValue, LT_OP);

    void *page = (char *) malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);
    ixFileHandle.fileHandle.readPage(root->children[pos], page);
    Node *node = new Node(ixFileHandle, root->attrType, page);
    free(page);
    node->pageNum = root->children[pos];
    routeToLeaf(ixFileHandle, route, node, attrValue);
    return 0;
}

RC IndexManager::scan(IXFileHandle &ixFileHandle,
                      const Attribute &attribute,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyKeyInclusive,
                      bool highKeyKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator) {
    if (!ixFileHandle.fileHandle.fileHandleOccupied()) {
        // Try to scan a file that doesn't exist
        return -12;
    }

    ix_ScanIterator.lowKey.readAttr(attribute.type, lowKey);
    ix_ScanIterator.highKey.readAttr(attribute.type, highKey);
    ix_ScanIterator.lowKeyInclusive = lowKeyKeyInclusive;
    ix_ScanIterator.highKeyInclusive = highKeyKeyInclusive;
    ix_ScanIterator.attrType = attribute.type;
    ix_ScanIterator.ixFileHandle = &ixFileHandle;
    return 0;
}

void IndexManager::printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const {
    if (ixFileHandle.fileHandle.getNumberOfPages() == 0) {
        return;
    }
    int pageNum = 0;
    printTreeNode(ixFileHandle, attribute, pageNum, 0);
    printf("\n");
}

void IndexManager::printTreeNode(IXFileHandle &ixFileHandle, const Attribute &attribute,
                                 int &pageNum, int indent) const {
    void *page = malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);
    ixFileHandle.fileHandle.readPage(pageNum, page);
    Node node = Node(ixFileHandle, attribute.type, page);
    if (node.nodeType == SingleRoot || node.nodeType == Leaf) {
        node.printNodePointers(indent);
    } else {
        printf("%*s%s", indent, "", "{\n");
        printf("%*s%s", indent, "", "\"keys\":[");
        node.printNodeKeys();
        printf("%*s%s", indent, "", "]\n");
        printf("%*s%s", indent, "", "\"children\":[\n");
        for (int i = 0; i < node.children.size(); i++) {
            printTreeNode(ixFileHandle, attribute, node.children[i], indent + 1);
            if (i != node.children.size() - 1) {
                printf(",");
                printf("\n");
            }
        }
        printf("\n");
        printf("%*s%s", indent, "", "]}\n");
    }
    free(page);
}

RC Node::printNodeKeys() {
    for (int i = 0; i < this->keys.size(); i++) {
        cout << "\"";
        this->keys[i].printSelf();
        cout << "\"";
        if (i != this->keys.size() - 1) {
            printf(",");
        }
    }
    return 0;
}

RC Node::printNodePointers(int indent) {
    printf("%*s%s", indent, "", "{\n");
    printf("%*s", indent + 1, "\"keys\": [");
    for (int i = 0; i < this->pointers.size(); i++) {
        printf("\"");
        this->keys[i].printSelf();
        printf("[");
        for (int j = 0; j < this->pointers[i].size(); j++) {
            printf("(%d, %d)", this->pointers[i][j].pageNum, this->pointers[i][j].slotNum);
            if (j != this->pointers[i].size() - 1)
                printf(",");
        }
        printf("]\"");
        if (i != this->keys.size() - 1)
            printf(",");
    }
    printf("]\n");
    printf("%*s%s", indent, "", "}\n");
    return 0;
}

IX_ScanIterator::IX_ScanIterator() {
    this->pageNum = 0;
    this->curK = 0;
    this->curR = -1;
    this->prevP = -1;
    this->prevK = -1;
    this->prevR = -1;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
    if (this->pageNum > this->ixFileHandle->fileHandle.getNumberOfPages() - 1) {
        return -1;
    }

    if (this->node == nullptr) {
        void *page = malloc(PAGE_SIZE);
        memset(page, 0, PAGE_SIZE);
        this->ixFileHandle->fileHandle.readPage(this->pageNum, page);
        this->node = new Node(*this->ixFileHandle, this->attrType, page);
        this->node->pageNum = this->pageNum;
        free(page);
    }

    if (this->node->nodeType != SingleRoot && this->node->nodeType != Leaf) {
        this->pageNum = reachLeaf();
    }
    void *page = malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);
    while (true) {
        this->curR++;
        if (!this->node->pointers.empty() && this->curR >= this->node->pointers[this->curK].size()) {
            this->curK++;
            this->curR = 0;
        }
        if (this->curK >= this->node->keys.size() && this->node->next != -1) {
            this->ixFileHandle->fileHandle.readPage(this->node->next, page);
            this->pageNum = this->node->next;
            delete this->node;
            this->node = new Node(*this->ixFileHandle, this->attrType, page);
            this->node->pageNum = this->pageNum;
            this->curK = 0;
            this->curR = 0;
        }

        if (this->curK >= this->node->keys.size() && this->node->next == -1) {
            free(page);
            return IX_EOF;
        }
        if (this->curK >= this->node->keys.size() || this->curR >= this->node->pointers[this->curK].size()) {
            continue;
        }

        if (this->lowKey.length > 0) {
            if (this->lowKeyInclusive) {
                if (AttrValue::compAttr(this->node->keys[this->curK], this->lowKey, LT_OP)) {
                    continue;
                }
            } else {
                if (AttrValue::compAttr(this->node->keys[this->curK], this->lowKey, LE_OP)) {
                    continue;
                }
            }
        }
        if (this->highKey.length > 0) {
            if (this->highKeyInclusive) {
                if (AttrValue::compAttr(this->node->keys[this->curK], this->highKey, GT_OP)) {
                    free(page);
                    return IX_EOF;
                }
            } else {
                if (AttrValue::compAttr(this->node->keys[this->curK], this->highKey, GE_OP)) {
                    free(page);
                    return IX_EOF;
                }
            }
        }
        rid.pageNum = this->node->pointers[this->curK][this->curR].pageNum;
        rid.slotNum = this->node->pointers[this->curK][this->curR].slotNum;
        this->node->keys[this->curK].writeAttr(key);
        if (this->prevP == this->pageNum) {
            this->ixFileHandle->fileHandle.readPage(this->pageNum, page);
            delete this->node;
            this->node = new Node(*this->ixFileHandle, this->attrType, page);
            this->node->pageNum = this->pageNum;
            RID lastRid = this->node->pointers[this->prevK][this->prevR];
            // In this situation, the prevRid has already been deleted
            if (lastRid.pageNum != this->prevRid.pageNum || lastRid.slotNum != this->prevRid.slotNum) {
                this->curK = this->prevK;
                this->curR = this->prevR;
                this->pageNum = this->prevP;
            }
        }
        this->prevRid = rid;
        this->prevP = this->pageNum;
        this->prevK = this->curK;
        this->prevR = this->curR;
        break;
    }
    free(page);
    return 0;
}

RC IX_ScanIterator::close() {
    this->pageNum = 0;
    this->curK = 0;
    this->curR = -1;
    if (this->node != nullptr) {
        delete this->node;
        this->node = nullptr;
    }
    return 0;
}

int IX_ScanIterator::reachLeaf() {
    int cPage = 0;
    int pos;
    void *page = (char *) malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);
    while (this->node->nodeType != SingleRoot && this->node->nodeType != Leaf) {
        if (this->lowKey.length > 0) {
            pos = this->node->locateChildPos(this->lowKey, LT_OP);
        } else {
            pos = 0;
        }
        cPage = this->node->children[pos];
        this->ixFileHandle->fileHandle.readPage(cPage, page);
        delete this->node;
        this->node = new Node(*this->ixFileHandle, this->attrType, page);
    }
    free(page);
    return cPage;
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

Node::~Node() {}

Node::Node(AttrType type) {
    this->attrType = type;
}

Node::Node(IXFileHandle &ixFileHandle, AttrType type, const void *page) {
    this->attrType = type;
    int offset = 0;
    memcpy(&this->nodeType, (char *) page + offset, sizeof(NodeType));
    offset += sizeof(NodeType);
    memcpy(&this->previous, (char *) page + offset, sizeof(int));
    offset += sizeof(int);
    memcpy(&this->next, (char *) page + offset, sizeof(int));
    offset += sizeof(int);

    if (this->nodeType == SingleRoot || this->nodeType == Root) {
        this->pageNum = 0;
    }

    // Copy keys to Node
    int nKeys;
    memcpy(&nKeys, (char *) page + offset, sizeof(int));
    offset += sizeof(int);
    AttrValue attrValue;
    for (int i = 0; i < nKeys; i++) {
        attrValue.readAttr(this->attrType, (char *) page + offset);
        offset += attrValue.length;
        this->keys.emplace_back(attrValue);
    }

    // Copy rids to Node
    if (this->nodeType == Leaf || this->nodeType == SingleRoot) {
        int nRids;
        memcpy(&nRids, (char *) page + offset, sizeof(int));
        offset += sizeof(int);
        for (int i = 0; i < nRids; i++) {
            int nRid;
            memcpy(&nRid, (char *) page + offset, sizeof(int));
            offset += sizeof(int);
            vector<RID> rids;
            RID rid;
            for (int j = 0; j < nRid; j++) {
                memcpy(&rid, (char *) page + offset, sizeof(RID));
                offset += sizeof(RID);
                rids.emplace_back(rid);
            }
            this->pointers.emplace_back(rids);
        }
    } else {
        int nChildren;
        memcpy(&nChildren, (char *) page + offset, sizeof(int));
        offset += sizeof(int);
        for (int i = 0; i < nChildren; i++) {
            int childPageNum;
            memcpy(&childPageNum, (char *) page + offset, sizeof(int));
            offset += sizeof(int);
            this->children.emplace_back(childPageNum);
        }
    }

    int overFlowPageNum;
    memcpy(&overFlowPageNum, (char *) page + offset, sizeof(int));
    if (overFlowPageNum != -1) {
        this->isOverflow = true;
        this->overFlowPages.emplace_back(overFlowPageNum);
        this->deserializeOverflowPage(ixFileHandle, overFlowPageNum);
    }
}

RC Node::deserializeOverflowPage(IXFileHandle &ixFileHandle, int nodePageNum) {
    void *page = malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);
    ixFileHandle.fileHandle.readPage(nodePageNum, page);
    int offset = 0;
    int nRids;
    memcpy(&nRids, (char *) page + offset, sizeof(int));
    offset += sizeof(int);
    for (int i = 0; i < nRids; i++) {
        RID rid;
        memcpy(&rid, (char *) page + offset, sizeof(RID));
        offset += sizeof(RID);
        this->pointers[0].emplace_back(rid);
    }
    int overFlowPageNum;
    memcpy(&overFlowPageNum, (char *) page + offset, sizeof(int));
    free(page);
    if (overFlowPageNum != -1) {
        this->overFlowPages.emplace_back(overFlowPageNum);
        this->deserializeOverflowPage(ixFileHandle, overFlowPageNum);
    }
    return 0;
}

RC Node::writeNode(IXFileHandle &ixFileHandle) {
    void *page = malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);
    if ((this->nodeType == SingleRoot || this->nodeType == Leaf) && this->keys.size() == 1 &&
        this->getNodeSize() > PAGE_SIZE) {
        int nRidInNode = (PAGE_SIZE - this->getHeaderSize()) / sizeof(RID);
        int remainSpace = this->pointers[0].size() - nRidInNode;
        int nRidInPage = (PAGE_SIZE - sizeof(RID)) / sizeof(RID);
        int required = remainSpace / nRidInPage + 1;
        if (required > this->overFlowPages.size()) {
            ixFileHandle.fileHandle.appendPage(page);
            this->overFlowPages.emplace_back(ixFileHandle.fileHandle.getNumberOfPages() - 1);
        }

        int offset = 0;
        memcpy((char *) page + offset, &this->nodeType, sizeof(NodeType));
        offset += sizeof(NodeType);
        memcpy((char *) page + offset, &this->previous, sizeof(int));
        offset += sizeof(int);
        memcpy((char *) page + offset, &this->next, sizeof(int));
        offset += sizeof(int);
        int nKeys = this->keys.size();
        memcpy((char *) page + offset, &nKeys, sizeof(int));
        offset += sizeof(int);
        for (int i = 0; i < nKeys; i++) {
            this->keys[i].writeAttr((char *) page + offset);
            offset += this->keys[i].length;
        }
        int nRids = this->pointers.size();
        memcpy((char *) page + offset, &nRids, sizeof(int));
        offset += sizeof(int);
        for (int i = 0; i < nRids; i++) {
            memcpy((char *) page + offset, &nRidInNode, sizeof(int));
            offset += sizeof(int);
            for (int j = 0; j < nRidInNode; j++) {
                memcpy((char *) page + offset, &this->pointers[i][j], sizeof(RID));
                offset += sizeof(RID);
            }
        }

        memcpy((char *) page + offset, &this->overFlowPages[0], sizeof(int));
        ixFileHandle.fileHandle.writePage(this->pageNum, page);

        for (int i = 0; i < this->overFlowPages.size(); i++) {
            if (i < this->overFlowPages.size() - 1) {
                int offset = this->serializeOverflowPage(i * nRidInPage + nRidInNode,
                                                         (i + 1) * nRidInPage + nRidInNode, page);
                memcpy((char *) page + offset, &this->overFlowPages[i + 1], sizeof(int));
            } else {
                int offset = this->serializeOverflowPage(i * nRidInPage + nRidInNode,
                                                         this->pointers[0].size(), page);
                int nextPage = -1;
                memcpy((char *) page + offset, &nextPage, sizeof(int));
            }
            ixFileHandle.fileHandle.writePage(this->overFlowPages[i], page);
        }
    } else {
        this->serialize(page);
        ixFileHandle.fileHandle.writePage(this->pageNum, page);
    }
    free(page);
    return 0;
}

RC Node::serialize(void *page) {
    int offset = 0;
    memcpy((char *) page + offset, &this->nodeType, sizeof(int));
    offset += sizeof(NodeType);
    memcpy((char *) page + offset, &this->previous, sizeof(int));
    offset += sizeof(int);
    memcpy((char *) page + offset, &this->next, sizeof(int));
    offset += sizeof(int);

    int nKeys = this->keys.size();
    memcpy((char *) page + offset, &nKeys, sizeof(int));
    offset += sizeof(int);
    for (int i = 0; i < nKeys; i++) {
        this->keys[i].writeAttr((char *) page + offset);
        offset += this->keys[i].length;
    }

    if (this->nodeType == Leaf || this->nodeType == SingleRoot) {
        int nRids = this->pointers.size();
        memcpy((char *) page + offset, &nRids, sizeof(int));
        offset += sizeof(int);
        for (int i = 0; i < nRids; i++) {
            int nRec = this->pointers[i].size();
            memcpy((char *) page + offset, &nRec, sizeof(int));
            offset += sizeof(int);
            for (int j = 0; j < nRec; j++) {
                memcpy((char *) page + offset, &this->pointers[i][j], sizeof(RID));
                offset += sizeof(RID);
            }
        }
    } else {
        int nChildren = this->children.size();
        memcpy((char *) page + offset, &nChildren, sizeof(int));
        offset += sizeof(int);
        for (int i = 0; i < nChildren; i++) {
            memcpy((char *) page + offset, &this->children[i], sizeof(int));
            offset += sizeof(int);
        }
    }

    int overFlowPage = -1;
    memcpy((char *) page + offset, &overFlowPage, sizeof(int));
    return 0;
}

int Node::serializeOverflowPage(int start, int end, void *page) {
    int nRid = end - start;
    int offset = 0;
    memcpy((char *) page + offset, &nRid, sizeof(int));
    offset += sizeof(int);
    for (int i = start; i < end; i++) {
        memcpy((char *) page + offset, &this->pointers[0][i], sizeof(RID));
        offset += sizeof(RID);
    }
    return offset;
}

int Node::locateChildPos(AttrValue &attrValue, CompOp compOp) {
    int i;
    for (i = 0; i < this->keys.size(); i++) {
        if (AttrValue::compAttr(attrValue, this->keys[i], compOp)) {
            return i;
        }
    }
    if (compOp == EQ_OP) {
        return -1;
    } else {
        return i;
    }
}

bool Node::checkKeyExist(const int &pos, const AttrValue &attrValue) {
    return !this->keys.empty() && pos > 0 && AttrValue::compAttr(attrValue, this->keys[pos - 1], EQ_OP);
}

void Node::insertKey(const int &pos, const AttrValue &attrValue) {
    if (!this->checkKeyExist(pos, attrValue)) {
        this->keys.insert(this->keys.begin() + pos, attrValue);
    }
}

void Node::insertPointer(const int &pos, const AttrValue &attrValue, const RID &rid) {
    if (this->checkKeyExist(pos, attrValue)) {
        this->pointers[pos - 1].emplace_back(rid);
    } else {
        vector<RID> rids;
        rids.emplace_back(rid);
        this->pointers.insert(this->pointers.begin() + pos, rids);
    }
}

void Node::insertChild(const int &pos, const int &pid) {
    this->children.insert(this->children.begin() + pos, pid);
}

int Node::getRightSibling(Node *parent, int &pos) {
    for (int i = 0; i < parent->children.size() - 1; i++) {
        if (parent->children[i] == this->pageNum) {
            pos = i;
            return parent->children[i + 1];
        }
    }
    return -1;
}

int Node::getLeftSibling(Node *parent, int &pos) {
    for (int i = 1; i < parent->children.size(); i++) {
        if (parent->children[i] == this->pageNum) {
            pos = i;
            return parent->children[i - 1];
        }
    }
    return -1;
}

int Node::getNodeSize() {
    int size = 0;
    size += sizeof(NodeType) + 2 * sizeof(int); // nodeType, previous, next
    int nKeys = this->keys.size();
    size += sizeof(int); // nKeys: this->keys.size()
    for (int i = 0; i < nKeys; i++) {
        size += this->keys[i].length;
    }

    if (this->nodeType == Leaf || this->nodeType == SingleRoot) {
        int nRids = this->pointers.size();
        size += sizeof(int); // nRids: this->pointer.size();
        for (int i = 0; i < nRids; i++) {
            size += sizeof(int); // nRecs: this->pointers[i].size();
            size += this->pointers[i].size() * sizeof(RID);
        }
    } else {
        size += sizeof(int); // this->children.size()
        size += this->children.size() * sizeof(int);
    }
    size += sizeof(int); // overflowPage
    this->size = size;
    return size;
}

int Node::getHeaderSize() {
    int size = 0;
    size += sizeof(NodeType) + 2 * sizeof(int); // nodeType, previous, next
    int nKeys = this->keys.size();
    size += sizeof(int); // number of keys
    for (int i = 0; i < nKeys; i++) {
        size += this->keys[i].length;
    }
    size += 3 * sizeof(int); // overflow pointer, nRec, nRids
    return size;
}

RC Node::deleteRecord(int pos, const RID &rid) {
    if (this->pointers[pos].size() > 1) {
        for (int i = 0; i < this->pointers[pos].size(); i++) {
            if (rid.pageNum == this->pointers[pos][i].pageNum && rid.slotNum == this->pointers[pos][i].slotNum) {
                this->pointers[pos].erase(this->pointers[pos].begin() + i);
                return 0;
            }
        }
        // Delete an rid that doesn't exist
        return -11;
    } else {
        if (rid.pageNum == this->pointers[pos][0].pageNum && rid.slotNum == this->pointers[pos][0].slotNum) {
            this->pointers.erase(this->pointers.begin() + pos, this->pointers.begin() + pos + 1);
            this->keys.erase(this->keys.begin() + pos);
            return 0;
        }
        // Delete an rid that doesn't exist
        return -11;
    }

}