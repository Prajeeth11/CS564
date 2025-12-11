#include "catalog.h"
#include "query.h"
#include <cstring>
#include <cstdlib>
#include <unordered_map>
#include <string>

/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string &relation,
                       const int attrCnt,
                       const attrInfo attrList[])
{
    cout << "Doing QU_Insert" << endl;

    Status status;
    AttrDesc *attrDesc;
    int relAttrCnt = 0;

    // Get relation attribute metadata
    status = attrCat->getRelInfo(relation, relAttrCnt, attrDesc);
    if (status != OK)
        return status;

    if (relAttrCnt != attrCnt)
        return OK;  // attribute count mismatch

    // Build a quick lookup for user-supplied attributes
    unordered_map<string, const attrInfo*> attrMap;
    for (int i = 0; i < attrCnt; i++) {
        attrMap[attrList[i].attrName] = &attrList[i];
    }

    // Compute total record size
    int recLen = 0;
    for (int i = 0; i < relAttrCnt; i++) {
        recLen += attrDesc[i].attrLen;
    }

    // Prepare insert file scan
    InsertFileScan resultRel(relation, status);
    if (status != OK)
        return status;

    // Allocate record buffer
    Record rec;
    rec.length = recLen;
    rec.data = (char*) malloc(recLen);

    // Fill record fields
    for (int i = 0; i < relAttrCnt; i++) {
        const string name = attrDesc[i].attrName;

        if (!attrMap.count(name))
            continue;  // should not happen if attrCnt==relAttrCnt

        const attrInfo* src = attrMap[name];
        char* dest = (char*)rec.data + attrDesc[i].attrOffset;

        if (src->attrType == INTEGER) {
            int val = atoi((char*)src->attrValue);
            memcpy(dest, &val, attrDesc[i].attrLen);
        }
        else if (src->attrType == FLOAT) {
            float val = atof((char*)src->attrValue);
            memcpy(dest, &val, attrDesc[i].attrLen);
        }
        else {
            memcpy(dest, src->attrValue, attrDesc[i].attrLen);
        }
    }

    // Insert the new record
    RID rid;
    resultRel.insertRecord(rec, rid);

    return OK;
}
