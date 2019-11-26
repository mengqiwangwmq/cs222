
#include "qe.h"

Filter::Filter(Iterator *input, const Condition &condition) {
    this->input = input;
    this->condition = condition;
    input->getAttributes(this->attrs);
    // Drop the relation name
    for(int i = 0; i < this->attrs.size(); i ++) {
        int pos = this->attrs[i].name.find('.');
        string attrName = this->attrs[i].name.substr(pos+1, this->attrs[i].name.length()-pos+1);
        this->attrs[i].name = attrName;
    }
}

RC Filter::getNextTuple(void *data) {
    while(input->getNextTuple(data) != RM_EOF) {
        void *filterValueData = malloc(PAGE_SIZE);
        int length = getFilterValue(filterValueData, data);
        if(length == -1) {
            // Invalid tuple
            return -1;
        }
        void *filterValue = malloc(length);
        memcpy(filterValue, filterValueData, length);
        free(filterValueData);
        bool satisfied = compare(filterValue, condition.rhsValue);
        if(satisfied) {
            return 0;
        }
    }
    return RM_EOF;
}

bool Filter::compare(void *filterValue, Value rhsValue) {
    bool satisfied = false;
    Attribute attr;
    attr.type = this->condition.rhsValue.type;
    Node node = Node(attr);
    switch(condition.op) {
        case EQ_OP:
        {
            bool rc = node.compareEqual(filterValue, rhsValue.data);
            if(rc == true) {
                satisfied = true;
            }
        }
            break;
        case LT_OP:
        {
            int rc = node.compareLess(filterValue, rhsValue.data);
            if(rc == 1) {
                satisfied = true;
            }
        }
            break;
        case LE_OP:
        {
            int rc = node.compareLess(filterValue, rhsValue.data);
            if(rc >= 0) {
                satisfied = true;
            }
        }
            break;
        case GT_OP:
        {
            int rc = node.compareLarge(filterValue, rhsValue.data);
            if(rc == 1) {
                satisfied = true;
            }
        }
            break;
        case GE_OP:
        {
            int rc = node.compareLarge(filterValue, rhsValue.data);
            if(rc >= 0) {
                satisfied = true;
            }
        }
            break;
        case NE_OP:
        {
            bool rc  = node.compareEqual(filterValue, rhsValue.data);
            if(rc != true) {
                satisfied = true;
            }
            break;
        }
        case NO_OP:
            satisfied = true;
            break;
    }
    return satisfied;
}

void Filter::getAttributes(std::vector<Attribute> &attrs) const {
    attrs.clear();
    attrs = this->attrs;
    for(int i = 0; i < this->attrs.size(); i ++) {
        string attrName = "Filter." + this->attrs[i].name;
        attrs[i].name = attrName;
    }
}

int Filter::getFilterValue(void *filterValueData, void *data) {
    int length = -1;
    int nullIndicatorSize = ceil((double)this->attrs.size()/CHAR_BIT);
    int offset = nullIndicatorSize;
    char *nullIndicator = (char *)malloc(nullIndicatorSize);
    memcpy(nullIndicator, data, nullIndicatorSize);
    int pos = this->condition.lhsAttr.find('.');
    string lhsAttrName = this->condition.lhsAttr.substr(pos+1, this->condition.lhsAttr.length()-pos+1);
    int i;
    for(i = 0; i < attrs.size(); i ++) {
        bool isNull = nullIndicator[i/CHAR_BIT] & (1<<(7-i%CHAR_BIT));
        if(isNull) {
            continue;
        }
        if(lhsAttrName == attrs[i].name) {
            if(attrs[i].type == TypeInt || attrs[i].type == TypeReal) {
                memcpy(filterValueData, (char *)data+offset, sizeof(int));
                length = sizeof(int);
            } else {
                int localLength;
                memcpy(&localLength, (char *)data+offset, sizeof(int));
                offset += sizeof(int);
                memcpy(filterValueData, &localLength, sizeof(int));
                memcpy((char *)filterValueData+ sizeof(int), (char *)data+offset, localLength);
                length = sizeof(int) + localLength;
            }
            return length;
        } else {
            if(attrs[i].type == TypeInt || attrs[i].type == TypeReal) {
                offset += sizeof(int);
            } else {
                int length;
                memcpy(&length, (char *)data+offset, sizeof(int));
                offset += sizeof(int) + length;
            }
        }
    }
    return -1;
}

// ... the rest of your implementations go here
