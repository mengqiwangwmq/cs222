#include "pfm.h"

PagedFileManager *PagedFileManager::_pf_manager = nullptr;

PagedFileManager &PagedFileManager::instance() {
    static PagedFileManager _pf_manager = PagedFileManager();
    return _pf_manager;
}

PagedFileManager::PagedFileManager() = default;

PagedFileManager::~PagedFileManager() { delete _pf_manager; }

PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

bool fileExist(const std::string &fileName) {
    FILE *file = fopen(fileName.c_str(), "r");
    if (file) {
        fclose(file);
        return true;
    } else {
        return false;
    }
}

RC PagedFileManager::createFile(const std::string &fileName) {
    if (fileExist(fileName)) {
        return -2; //FileDuplicateException
    }
    std::ofstream fs(fileName);
    char *hiddenPage = new char[PAGE_SIZE];

    unsigned cache = 0;
    // Allocate space for readPageCounter
    memcpy(hiddenPage, &cache, sizeof(unsigned));
    // Allocate space for writePageCounter
    memcpy(hiddenPage + sizeof(unsigned), &cache, sizeof(unsigned));
    // Allocate space for appendPageCounter
    memcpy(hiddenPage + 2 * sizeof(unsigned), &cache, sizeof(unsigned));

    fs.write(hiddenPage, PAGE_SIZE);
    fs.close();
    return 0;
}

RC PagedFileManager::destroyFile(const std::string &fileName) {
    if (!fileExist(fileName)) {
        return -1; //FileNotFoundException
    }
    remove(fileName.c_str());
    return 0;
}

RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
    if (!fileExist(fileName)) {
        return -1; //FileNotFoundException
    }
    RC flag = fileHandle.setFile(fileName);
    if (flag != 0) {
        return flag;
    }
    //TODO: implement read counter
}

RC PagedFileManager::closeFile(FileHandle &fileHandle) {
    // return -1;
    fileHandle.fhfs.close();
}

FileHandle::FileHandle() {
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
}

FileHandle::~FileHandle() = default;

RC FileHandle::readPage(PageNum pageNum, void *data) {
    // Check if page requested exists or not
    if(pageNum > this->getNumberOfPages()) {
        return -1;
    }

    // Skip hiddenPage
    fhfs.seekg((pageNum+1) * PAGE_SIZE);
    char * ptr = static_cast<char *>(data);
    fhfs.read(ptr, PAGE_SIZE);

    // Update counters
    this->readPageCounter ++;
    fhfs.seekg(0);
    fhfs.write((char*)readPageCounter, sizeof(unsigned));
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {
    return -1;
}

RC FileHandle::appendPage(const void *data) {
    return -1;
}

unsigned FileHandle::getNumberOfPages() {
    return -1;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    return -1;
}

RC FileHandle::setFile(const std::string &fileName) {
    if (fileHandleOccupied()) {
        return -3; //FStreamOccupiedException
    }
    try {
        fs.open(fileName);
    } catch (std::fstream::failure &err) {
        return 1;
    }
    return 0;
}
bool FileHandle::fileHandleOccupied() {
    if (fs) {
        return false;
    } else {
        return true;
    }
}