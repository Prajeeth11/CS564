#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string &result,
                       const int projCnt,
                       const attrInfo projNames[],
                       const attrInfo *attr,
                       const Operator op,
                       const char *attrValue)
{
    cout << "Doing QU_Select" << endl;

    Status status;
    AttrDesc attrDescArray[projCnt];  // hold projection info
    AttrDesc *filterAttr = nullptr;   // attribute to filter on
    int reclen = 0;

    // Get AttrDesc for each projected attribute
    for (int i = 0; i < projCnt; i++)
    {
        status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, attrDescArray[i]);
        if (status != OK)
            return status;

        reclen += attrDescArray[i].attrLen;
    }

    // Get AttrDesc for filter attribute if provided
    if (attr != nullptr)
    {
        filterAttr = new AttrDesc;
        status = attrCat->getInfo(attr->relName, attr->attrName, *filterAttr);
        if (status != OK)
            return status;
    }

    // Call ScanSelect to perform the actual scan and projection
    status = ScanSelect(result, projCnt, attrDescArray, filterAttr, op, attrValue, reclen);

    delete filterAttr; // cleanup filterAttr
    return status;
}

const Status ScanSelect(const string &result,
                        const int projCnt,
                        const AttrDesc projNames[],
                        const AttrDesc *filterAttr,
                        const Operator op,
                        const char *filter,
                        const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

    Status status;
    Record outputRec;
    Record inputRec;
    RID rid;

    // Create result relation scan
    InsertFileScan resultRel(result, status);
    if (status != OK)
        return status;

    outputRec.length = reclen;
    outputRec.data = new char[reclen]; 

    // Create scan on the input table
    HeapFileScan scan(projNames[0].relName, status);
    if (status != OK)
        return status;

    // Prepare filter value based on type
    char *scanValue = nullptr;
    int intVal;
    float floatVal;

    if (filterAttr == nullptr)
    {
        status = scan.startScan(0, 0, STRING, nullptr, EQ);
    }
    else
    {
        switch (filterAttr->attrType)
        {
        case INTEGER:
            intVal = atoi(filter);
            scanValue = (char *)&intVal;
            status = scan.startScan(filterAttr->attrOffset, filterAttr->attrLen, INTEGER, scanValue, op);
            break;

        case FLOAT:
            floatVal = atof(filter);
            scanValue = (char *)&floatVal;
            status = scan.startScan(filterAttr->attrOffset, filterAttr->attrLen, FLOAT, scanValue, op);
            break;

        default:
            scanValue = (char *)filter;
            status = scan.startScan(filterAttr->attrOffset, filterAttr->attrLen, STRING, scanValue, op);
            break;
        }
    }

    if (status != OK)
        return status;

    while (scan.scanNext(rid) == OK)
    {
        status = scan.getRecord(inputRec);
        if (status != OK)
            return status;

        int outputOffset = 0;
        for (int i = 0; i < projCnt; i++)
        {
            memcpy((char *)outputRec.data + outputOffset,
                   (char *)inputRec.data + projNames[i].attrOffset,
                   projNames[i].attrLen);

            outputOffset += projNames[i].attrLen;
        }

        RID outRID;
        status = resultRel.insertRecord(outputRec, outRID);
        if (status != OK)
            return status;
    }

    if (status != FILEEOF)
        return status;

    cout << "Scan finished" << endl;
    status = scan.endScan();
    delete[] outputRec.data;

    return status;
}
