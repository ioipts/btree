#include "axisbtree.h"

#define TESTNUM 1000000
#define TESTFILE "test.btree"

int main(int argc, char** argv)
{
	axisbtree b = createbtree(TESTFILE, 8, 64, AXISBTREEPLUSTYPE, 0);
	for (int i = 0; i < TESTNUM; i++)
	{
		insertbtree(b, "data", i);
	}
	destroybtree(b);
	b = openbtree(TESTFILE);
	for (int i = 200; i < 800; i++)
	{
		deletebtree(b, "data", i);
	}
	destroybtree(b);

	b = openbtree(TESTFILE);
	for (int i = 799; i >= 200; i--)
	{
		insertbtree(b, "data", i);
	}
	DATABASETYPE value[1000];
	int ret=selectbtree(b, "data", 790, 20, value);
	for (int i = 0; i < ret; i++)
	{
		printf("%d ", value[i]);
	}
	ret=countrangebtree(b,"data","data");
	ret=selectrangebtree(b,"data","data",790,20,value);
	printf("\n");
	destroybtree(b);
	return 0;
}