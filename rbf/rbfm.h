#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>
#include <iostream>
#include <cmath>
#include <cstring>
#include <cassert>

#include "pfm.h"

using namespace std;

// Record ID
typedef struct {
    unsigned pageNum;    // page number
    unsigned slotNum;    // slot number in the page
} RID;

// Attribute
typedef enum {
    TypeInt = 0, TypeReal, TypeVarChar
} AttrType;

typedef unsigned AttrLength;

struct Attribute {
    string name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum {
    EQ_OP = 0, // no condition// =
    LT_OP,      // <
    LE_OP,      // <=
    GT_OP,      // >
    GE_OP,      // >=
    NE_OP,      // !=
    NO_OP       // no condition
} CompOp;


/********************************************************************
* The scan iterator is NOT required to be implemented for Project 1 *
********************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

class AttrValue {
public:
    AttrType type;
    int length;
    string name;
    string vchar;
    int itg;
    float flt;

    AttrValue(int v) : type(TypeInt), length(sizeof(int)), vchar(""), itg(v), flt(0) {}

    AttrValue(float v) : type(TypeReal), length(sizeof(float)), vchar(""), itg(0), flt(v) {}

    AttrValue(string v) : type(TypeVarChar), length(sizeof(int) + (int) v.length()), vchar(v), itg(0), flt(0) {}

    AttrValue() : length(0), vchar(""), itg(0), flt(0) {}

    AttrValue(const Attribute &attr) : name(attr.name), type(attr.type) {}

    void readAttr(AttrType attrType, const void *data);

    void writeAttr(void *data);

    static bool compAttr(AttrValue left, AttrValue right, CompOp op);

    void printSelf();
};

inline bool operator==(const AttrValue &lhs, const AttrValue &rhs) {
    return lhs.flt == rhs.flt && lhs.itg == rhs.itg && lhs.vchar == rhs.vchar;
}

inline bool operator!=(const AttrValue &left, const AttrValue &right) { return !operator==(left, right); }

inline bool operator<(const AttrValue &left, const AttrValue &right) {
    return left.flt < right.flt || left.itg < right.itg || left.vchar < right.vchar ||
           (left.length == 0 && right.length != 0);
} // length = 0 means -inifinity
inline bool operator>(const AttrValue &left, const AttrValue &right) { return operator<(right, left); }

inline bool operator<=(const AttrValue &left, const AttrValue &right) { return !operator>(left, right); }

inline bool operator>=(const AttrValue &left, const AttrValue &right) { return !operator<(left, right); }

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

class RBFM_ScanIterator {
public:
    RBFM_ScanIterator();

    ~RBFM_ScanIterator() = default;

    void init(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
              const CompOp compOp, const void *value, const vector<string> &attrNames);

    // Never keep the results in the memory. When getNextRecord() is called,
    // a satisfying record needs to be fetched from the file.
    // "data" follows the same format as RecordBasedFileManager::insertRecord().
    RC getNextRecord(RID &rid, void *data);

    bool checkSatisfied(void *checkValue);

    RC close();

    int pageNum;
    short slotNum;
    FileHandle *fileHandle;
    vector<Attribute> recordDescriptor;
    CompOp compOp;
    const void *value;
    vector<short> attrIdx;
    vector<string> attrNames;
    short condAttrIdx;
};

class RecordBasedFileManager {
public:
    static RecordBasedFileManager &instance();                          // Access to the _rbf_manager instance

    RC createFile(const string &fileName);                         // Create a new record-based file

    RC destroyFile(const string &fileName);                        // Destroy a record-based file

    RC openFile(const string &fileName, FileHandle &fileHandle);   // Open a record-based file

    RC closeFile(FileHandle &fileHandle);                               // Close a record-based file

    //  Format of the data passed into the function is the following:
    //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
    //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
    //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
    //     Each bit represents whether each field value is null or not.
    //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
    //     If k-th bit from the left is set to 0, k-th field contains non-null values.
    //     If there are more than 8 fields, then you need to find the corresponding byte first,
    //     then find a corresponding bit inside that byte.
    //  2) Actual data is a concatenation of values of the attributes.
    //  3) For Int and Real: use 4 bytes to store the value;
    //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
    //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
    // For example, refer to the Q8 of Project 1 wiki page.

    short getNullFlagSize(int fieldCount);

    static short getPageSlotTotal(const void *page);

    static void setPageSlotTotal(const void *page, short slotTotal);

    static short getPageFreeSpace(const void *page);

    static void setPageFreeSpace(const void *page, short space);

    static short getRecordOffset(const void *page, short slotNum);

    static void setRecordOffset(const void *page, short offset, short slotNum);

    static short getRecordSize(const void *page, short slotNum);

    static void setRecordSize(const void *page, short recordSize, short slotNum);

    void getAttributeOffset(const void *page, short pagePtr, int fieldCount, short nullFlagSize,
                            short attrIdx, short &offset, short &prevOffset);

    short getInsertPtr(const void *page);

    short countRemainSpace(const void *page, short freeSpace, short recordSize, bool newFlag);

    short findFreeSlot(const void *page);

    short parseRecord(const vector<Attribute> &recordDescriptor, const void *data, const void *offsetTable);

    RC copyRecord(const void *page, short insertPtr, int fieldCount, const void *data, const void *offsetTable,
                  short recordSize);

    // Insert a record into a file
    RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);

    // Read a record identified by the given rid.
    RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);

    void locateRecord(FileHandle &fileHandle, void *page, short &recordOffset, short &recordSize, RID *&id);

    void shiftRecord(const void *page, short recordOffset, short distance);

    // Print the record that is passed to this utility method.
    // This method will be mainly used for debugging/testing.
    // The format is as follows:
    // field1-name: field1-value  field2-name: field2-value ... \n
    // (e.g., age: 24  height: 6.1  salary: 9000
    //        age: NULL  height: 7.5  salary: 7500)
    RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

    /*****************************************************************************************************
    * IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) *
    * are NOT required to be implemented for Project 1                                                   *
    *****************************************************************************************************/
    // Delete a record identified by the given rid.
    RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

    // Assume the RID does not change after an update
    RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data,
                    const RID &rid);

    // Read an attribute given its name and the rid.
    RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid,
                     const string &attributeName, void *data);

    // Scan returns an iterator to allow the caller to go through the results one by one.
    RC scan(FileHandle &fileHandle,
            const vector<Attribute> &recordDescriptor,
            const string &conditionAttribute,
            const CompOp compOp,                  // comparision type such as "<" and "="
            const void *value,                    // used in the comparison
            const vector<string> &attrNames, // a list of projected attributes
            RBFM_ScanIterator &rbfm_ScanIterator);

protected:
    RecordBasedFileManager();                                                   // Prevent construction
    ~RecordBasedFileManager();                                                  // Prevent unwanted destruction
    RecordBasedFileManager(const RecordBasedFileManager &);                     // Prevent construction by copying
    RecordBasedFileManager &operator=(const RecordBasedFileManager &);          // Prevent assignment


private:
    static RecordBasedFileManager *_rbf_manager;
    PagedFileManager *_pf_manager;
};

#endif