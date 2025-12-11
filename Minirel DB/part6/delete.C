#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string &relation,
                       const string &attrName,
                       const Operator op,
                       const Datatype type,
                       const char *attrValue)
{
    if (relation.empty())
        return BADCATPARM;

    Status status;
    RID rid;
    HeapFileScan *scanner = new HeapFileScan(relation, status);
    if (status != OK)
        return status;

    // If no attribute is specified, delete all records
    if (attrName.empty())
    {
        status = scanner->startScan(0, 0, STRING, nullptr, EQ);
        if (status != OK)
            return status;

        while ((status = scanner->scanNext(rid)) != FILEEOF)
        {
            status = scanner->deleteRecord();
            if (status != OK)
                return status;
        }
    }
    else
    {
        // Get attribute information
        AttrDesc attrD;
        status = attrCat->getInfo(relation, attrName, attrD);
        if (status != OK)
            return status;

        // Prepare the attribute value for scanning
        char *scanValue = nullptr;
        int intVal;
        float floatVal;

        switch (type)
        {
        case INTEGER:
            intVal = atoi(attrValue);
            scanValue = (char *)&intVal;
            break;
        case FLOAT:
            floatVal = atof(attrValue);
            scanValue = (char *)&floatVal;
            break;
        default: 
            scanValue = (char *)attrValue;
            break;
        }

        status = scanner->startScan(attrD.attrOffset, attrD.attrLen, type, scanValue, op);
        if (status != OK)
            return status;

        while ((status = scanner->scanNext(rid)) == OK)
        {
            status = scanner->deleteRecord();
            if (status != OK)
                return status;
        }
    }

    if (status != FILEEOF)
        return status;

    scanner->endScan();
    delete scanner;

    return OK;
}



