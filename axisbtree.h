#ifndef AXIXBTREE_H_
#define AXISBTREE_H_

/**
* Adaptive B-Tree for index searching on file
* 
* Memcmp (can be used with string or integer)
* Balancing when insert and delete.
* Iterative procedure. No recursive.
* key always store n+1 (n+1 is the last node)
* Using Binary Search 
*
* Insert at leaf only.
* The tree grow up not down. When leaf is full, seperate 2 groups of the leaf and move the pivot up one level. 
*
* 1
*
* 1 2 
*
* 1 2 3
*
* 1 2 3 4 
* 
*    3
* 1 2 4 5
*
*    3
* 1 2 4 5 6 7 
*    
*    3   6
* 1 2 4 5 7 8 
*
* 
* Delete at leaf only. 
* When delete, the tree will fall down. Try to merge sibling + parent down.
*
*/

#include "databasetype.h"

/**
* maximum key size
*/
#define AXISBTREEKEYSIZE 1024

/**
* Unique key or not
*/
#define AXISBTREEUNIQUETYPE 1
/**
* Insert num of child
*/
#define AXISBTREEPLUSTYPE 2

#pragma pack(push,1)

typedef struct axisbtreefileS* axisbtreefile;
struct axisbtreefileS {
	/**
	* - AXISTABLEVERSIONBIT32 or AXISTABLEVERSIONBIT64
	*/
	unsigned short version;
	/**
	* to check whether file is corrupt or not
	* - AXISTABLESTATUSNORMAL or AXISTABLESTATUSWRITE
	*/
	unsigned short status;
	/**
	* - AXISBTREEUNIQUETYPE, AXISBTREESORTEDTYPE 
	*/
	unsigned short type;
	/**
	* size of each key (4-1024)
	*/
	unsigned short keysize;
	/**
	* maximum num of key in a node
	*/
	unsigned short numkey;
	/**
	* total num of entries
	*/
	DATABASETYPE numnode;
	/**
	* so far maximum level
	*/
	DATABASETYPE maxlevel;
	/**
	* last block
	*/
	DATABASETYPE numblock;
} PACKED;

typedef struct axisbtreenodeS* axisbtreenode;
struct axisbtreenodeS {
	DATABASETYPE num;			//number of child
	DATABASETYPE child;			//seek in file for child
	DATABASETYPE data;			//data
	char key[AXISBTREEKEYSIZE];	//must be 32bit aligned 
} PACKED;

typedef struct axisbtreeblockS* axisbtreeblock;
struct axisbtreeblockS {
	DATABASETYPE num;			//number of node (alignment data is faster)
	DATABASETYPE parentnode;	//use this for delete
	//axisbtreenode[]
} PACKED;

typedef struct axisbtreeS* axisbtree;
struct axisbtreeS {
	FILEPTR f;
	struct axisbtreefileS header;
	axisbtreeblock block;
} PACKED;

#pragma pack(pop)

/**
* create balance btree 
* @param filename including path
* @param keysize the size needs to be align (8,64,...)
* @param numkey must be even
* @param initsize of index file
* @param type  AXISBTREEUNIQUETYPE | AXISBTREESORTEDTYPE 
* @return NULL if keysize>AXISMAXKEYSIZE or numkey is odd
*/
axisbtree createbtree(const char* filename, unsigned short keysize, unsigned short numkey, unsigned short type, DATABASETYPE initsize);

/**
* open balance btree
* @param filename including path
*/
axisbtree openbtree(const char* filename);

/**
* must call destroy btree to set unlock 
*/
void destroybtree(axisbtree btree);

/**
* set write flag
*/
void lockbtree(axisbtree btree);

/**
* set normal flag
*/
void unlockbtree(axisbtree btree);

/**
* insert with balancing
* 
* 3 cases
* - insert at other level, just create a child to be the leaf level, then insert
* - insert at leaf level
*	> simple case, insert at leaf level
*   > insert at full leaf level, then grow up the tree. 
* 
* The tree grow up not down!
* 
* @return true if the key not exists
*/
bool insertbtree(axisbtree btree,const char* key,DATABASETYPE id);

/**
* key can't be duplicated
* @param update if key exists and update is true then update the data 
* @return true if not exists
*/
bool insertuniquebtree(axisbtree btree, const char* key, DATABASETYPE id,bool update);

/**
* delete with balancing
* 
* - First, find the key.
* - If that node is a leaf, then simply remove
* - If that node is not a leaf, then find max key or min key to replace that node.
* - fall down process. Try to merge sibling block.
* 
* The tree fall down. Not remove the key from leaf, remove from the top.
* @return true if the key exists
*/
bool deletebtree(axisbtree btree, const char* key, DATABASETYPE id);
bool deleteuniquebtree(axisbtree btree, const char* key);

/**
* @return the num of item in "value" array 
*/
DATABASETYPE selectbtree(axisbtree btree, const char* key,DATABASETYPE offset,DATABASETYPE limit,DATABASETYPE* value);
DATABASETYPE selectrangebtree(axisbtree btree, const char* keyfrom, const char* keyto, DATABASETYPE offset, DATABASETYPE limit, DATABASETYPE* value);
DATABASETYPE countrangebtree(axisbtree btree, const char* keyfrom,const char* keyto);

/**
* @return true if found
*/
bool selectuniquebtree(axisbtree btree, const char* key,DATABASETYPE* ret);
DATABASETYPE selectuniquerangebtree(axisbtree btree, const char* keyfrom, const char* keyto, DATABASETYPE offset, DATABASETYPE limit, DATABASETYPE* value);
DATABASETYPE countuniquerangebtree(axisbtree btree, const char* keyfrom, const char* keyto);

/**
* for debug
*/
void printbtree(const char* filename,int numblock);

#endif