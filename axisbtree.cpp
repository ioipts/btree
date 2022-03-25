#include "axisbtree.h"

#define BTREEHEADERSIZE sizeof(struct axisbtreefileS)
#define BTREEGETNODE(p,i,k) ((axisbtreenode)((char*)(p)+sizeof(struct axisbtreeblockS)+(BTREENODESIZE(k)*(i))))
#define BTREESEEKNODE(i,k) (sizeof(struct axisbtreeblockS)+(BTREENODESIZE(k)*(i)))
#define BTREEBLOCKSIZE(k,n) (sizeof(struct axisbtreeblockS)+((n)+1)*(sizeof(struct axisbtreenodeS)-AXISBTREEKEYSIZE+(k)))
#define BTREENODESIZE(k) (sizeof(struct axisbtreenodeS)-AXISBTREEKEYSIZE+(k))
#define BTREECOMPARE(c,k1,d1,k2,d2,s) (((c=memcmp(k1,k2,s))==0)?((d1==d2)?c=0:((d1<d2)?c=-1:c=1)):c)
#define BTREEWRITESTATUS(b,s) b->header.status = s;\
		FSEEK(b->f, 0, SEEK_SET); \
		FWRITE(&b->header, BTREEHEADERSIZE, 1, b->f);	\
		FFLUSH(b->f); \

struct btreestack {
	DATABASETYPE seek;
	int i;
	DATABASETYPE rmseek;
	axisbtreeblock block;
};

axisbtree createbtree(const char* filename, unsigned short keysize, unsigned short numkey, unsigned short type, DATABASETYPE initsize)
{
	struct axisbtreefileS file;
	if ((keysize > AXISBTREEKEYSIZE) || ((numkey & 1) == 1)) return NULL;
	file.version = (sizeof(DATABASETYPE) == 4) ? AXISTABLEVERSIONBIT32 : AXISTABLEVERSIONBIT64;
	file.status = AXISTABLESTATUSNORMAL;
	file.keysize = keysize;
	file.numkey = numkey;
	file.type = type;
	file.maxlevel = 0;
	file.numnode = 0;
	file.numblock = 1;			//always 1 block
	FILEPTR f= FOPEN(filename, "wb");
	if (f == NULL) return NULL;
	FWRITE(&file, sizeof(file), 1, f);
	int blocksize = BTREEBLOCKSIZE(keysize, numkey);
	axisbtreeblock b = (axisbtreeblock)ALLOCMEM(blocksize);
	SETMEM(b, 0, blocksize);
	FWRITE(b, blocksize, 1, f);
	FREEMEM(b);
	//expand
	if (initsize > (BTREEHEADERSIZE + BTREEBLOCKSIZE(keysize, numkey))) {
#define BTREETEMPSIZE 1000000
		char* temp = (char*)ALLOCMEM(BTREETEMPSIZE);
		size_t t = 0;
		size_t total = initsize - (BTREEHEADERSIZE + BTREEBLOCKSIZE(keysize, numkey));
		while (t < total) {
			size_t n = (t + BTREETEMPSIZE > total)? total - t: BTREETEMPSIZE;
			FWRITE(temp, n,1, f);
			t += n;
		}
		FREEMEM(temp);
	}
	FCLOSE(f);
	return openbtree(filename);
}

axisbtree openbtree(const char* filename)
{
	struct axisbtreefileS file;
	FILEPTR f = FOPEN(filename, "rb+");
	if (f == NULL) return NULL;
	FREAD(&file, BTREEHEADERSIZE, 1, f);	
	unsigned short version = (sizeof(DATABASETYPE) == 4) ? AXISTABLEVERSIONBIT32 : AXISTABLEVERSIONBIT64;
	if ((file.version & AXISTABLEVERSIONBITMASK) != version) {
		FCLOSE(f);
		return NULL;
	}
	axisbtree btree = (axisbtree)ALLOCMEM(sizeof(struct axisbtreeS));
	btree->f = f;
	btree->header = file;
	btree->block = (axisbtreeblock)ALLOCMEM(BTREEBLOCKSIZE(file.keysize,file.numkey));
	return btree;
}

void destroybtree(axisbtree btree)
{
	unlockbtree(btree);
	FCLOSE(btree->f);
	FREEMEM(btree->block);
	FREEMEM(btree);
}

struct btreestack* initbtreestack(axisbtreeblock first,DATABASETYPE blocksize,int l)
{
	struct btreestack* s=(struct btreestack*)ALLOCMEM(sizeof(struct btreestack) * (l + 3)); //add up for insert
	s[0].seek = BTREEHEADERSIZE;
	s[0].i = 0;
	s[0].block = first;
	for (int i = 1; i < l+3; i++)
	{
		s[i].block = (axisbtreeblock)ALLOCMEM(blocksize);
	}
	return s;
}

void destroybtreestack(struct btreestack* s,int l)
{
	for (int i = 1; i < l+3; i++)
	{
		FREEMEM(s[i].block);
	}
	FREEMEM(s);
}

FORCE_INLINE void btreeshift(axisbtree btree, struct btreestack* stack, int levelindex,int inc)
{   //update the num of child (inc=1 or -1)
	unsigned short keysize = btree->header.keysize;
	DATABASETYPE blocksize = BTREEBLOCKSIZE(btree->header.keysize, btree->header.numkey);
	levelindex--;
	while (levelindex>=0) {
		axisbtreeblock block = stack[levelindex].block;
		int i = stack[levelindex].i;
		axisbtreenode node = BTREEGETNODE(block, i, keysize);
		node->num += inc;
		FSEEK(btree->f, stack[levelindex].seek + BTREESEEKNODE(i, keysize), SEEK_SET);
		FWRITE(&node->num, sizeof(DATABASETYPE), 1, btree->f);
		levelindex--;
	}
}

FORCE_INLINE bool btreegrowup(axisbtree btree,struct btreestack* stack, int levelindex, const char* key, DATABASETYPE id)
{	
	int keysize = btree->header.keysize;
	int numkey = btree->header.numkey;
	int maxlevel = btree->header.maxlevel;
	int blocksize = BTREEBLOCKSIZE(btree->header.keysize, btree->header.numkey);
	int numlevelindex = levelindex;
	//simply add
	axisbtreeblock block = stack[levelindex].block;
	axisbtreenode plastnode = BTREEGETNODE(block, block->num, keysize);
	struct axisbtreenodeS lastnode;
	CPYMEM(&lastnode,plastnode,BTREENODESIZE(keysize));
	if (block->num != numkey) {		//copy the last node
		CPYMEM(BTREEGETNODE(block, block->num + 1, keysize),plastnode,BTREENODESIZE(keysize));
	}
	int c= -1;
	int i = block->num-1;
	axisbtreenode n=BTREEGETNODE(block, (i>=0)?i:0, keysize);
	while ((i>=0) && ((BTREECOMPARE(c, key, id, (const char*)n->key, n->data, keysize)) < 0)) {
		//shift
		axisbtreenode n2 = BTREEGETNODE(block, (i + 1), keysize);
		CPYMEM(n2, n, BTREENODESIZE(keysize));
		i--;
		n = BTREEGETNODE(block, (i>=0)?i:0, keysize);
	}
	if (c == 0) return false;		//exists
	if (i >= 0) {					//not empty, get the space
		n = BTREEGETNODE(block, i+1, keysize);
	}
	n->data = id;
	CPYMEM(n->key, key, keysize);
	n->child = n->num = 0;
	block->num++;
	while (block->num>numkey) {
		if ((levelindex==0) || (levelindex+3<maxlevel)) {			//balance down(for root and level not equal)
			int left=levelindex+1;
			int right=levelindex+2;
			int half = block->num / 2;
			block->num = 1;
			axisbtreenode lnode = BTREEGETNODE(block, 0, keysize);		//first node
			axisbtreenode rnode = BTREEGETNODE(block, 1, keysize);		//last node
			axisbtreenode pnode = BTREEGETNODE(block, half, keysize);	//pivot node
			//do left 
			axisbtreeblock lblock=stack[left].block;
			lblock->parentnode=stack[levelindex].seek;
			lblock->num=btree->header.numkey-half;
			CPYMEM(BTREEGETNODE(lblock,0,keysize),BTREEGETNODE(block,0,keysize), BTREENODESIZE(keysize)*(lblock->num));
			//last node for left
			axisbtreenode llastnode = BTREEGETNODE(lblock, lblock->num, keysize);
			SETMEM(llastnode, 0, BTREENODESIZE(keysize));
			llastnode->child = pnode->child;
			llastnode->num = pnode->num;
			//do right
			axisbtreeblock rblock=stack[right].block;	
			rblock->parentnode=stack[levelindex].seek;
			rblock->num=half;
			CPYMEM(BTREEGETNODE(rblock,0,keysize), BTREEGETNODE(block, half+1, keysize), BTREENODESIZE(keysize)*(rblock->num));
			//last node for right
			CPYMEM(BTREEGETNODE(rblock, rblock->num, keysize), &lastnode, BTREENODESIZE(keysize));
			//do parent (only 1 node)
			lnode->data = pnode->data;
			CPYMEM(lnode->key, pnode->key, keysize);
			lnode->child=BTREEHEADERSIZE+btree->header.numblock*blocksize;
			rnode->data = 0;
			SETMEM(rnode->key, 0, keysize);
			rnode->child=BTREEHEADERSIZE+(btree->header.numblock+1)*blocksize;
			//update numofchild
			if ((btree->header.type & AXISBTREEPLUSTYPE) == AXISBTREEPLUSTYPE) {
				lnode->num = 0;
				for (int i = 0; i <=lblock->num; i++) {
					axisbtreenode node = BTREEGETNODE(lblock, i, keysize);
					lnode->num += node->num + ((i == lblock->num) ? 0 : 1);
				}
				rnode->num = 0;
				for (int i = 0; i <=rblock->num ; i++) {
					axisbtreenode node = BTREEGETNODE(rblock, i, keysize);
					rnode->num += node->num + ((i == rblock->num) ? 0 : 1);
				}
			}
			//update parent later
			FSEEK(btree->f,lnode->child,SEEK_SET);
			FWRITE(lblock,blocksize,1,btree->f);
			FSEEK(btree->f,rnode->child,SEEK_SET);
			FWRITE(rblock,blocksize,1,btree->f);
			//update parentnode of childs (no need if insert-only btree)
			axisbtreenode n = BTREEGETNODE(lblock, 0, keysize); 
			for (int i = 0; i <= lblock->num; i++)
			{
				if (n->child != 0) {
					FSEEK(btree->f, n->child+sizeof(DATABASETYPE), SEEK_SET);
					FWRITE(&lnode->child,sizeof(DATABASETYPE),1,btree->f);
				}
				n = (axisbtreenode)(((char*)n)+BTREENODESIZE(keysize));
			}
			n = BTREEGETNODE(rblock, 0, keysize);
			for (int i = 0; i <= rblock->num; i++)
			{
				if (n->child != 0) {
					FSEEK(btree->f, n->child + sizeof(DATABASETYPE), SEEK_SET);
					FWRITE(&rnode->child, sizeof(DATABASETYPE), 1, btree->f);
				}
				n = (axisbtreenode)(((char*)n) + BTREENODESIZE(keysize));
			}
			btree->header.numblock+=2;					//always end
			if (numlevelindex+1 > btree->header.maxlevel) btree->header.maxlevel = numlevelindex+1;
		} else {										//balance up
			int half = btree->header.numkey / 2;
			int left = levelindex + 1;
			int right = levelindex;
			//new node for the left
			stack[left].seek = BTREEHEADERSIZE + (btree->header.numblock * blocksize);
			btree->header.numblock++;
			axisbtreeblock lblock = stack[left].block;
			//previous node is right 
			axisbtreeblock rblock = stack[levelindex].block;
			//do left
			lblock->num = btree->header.numkey - half;
			lblock->parentnode = stack[levelindex - 1].seek;
			axisbtreenode lnode = BTREEGETNODE(lblock, 0, keysize);
			axisbtreenode rnode = BTREEGETNODE(rblock, 0, keysize);
			CPYMEM(lnode,rnode, BTREENODESIZE(keysize)*lblock->num);
			//last node for left
			axisbtreenode llastnode = BTREEGETNODE(lblock, lblock->num, keysize);	
			SETMEM(llastnode, 0, BTREENODESIZE(keysize));
			//copy pivot
			struct axisbtreenodeS pivot;
			CPYMEM(&pivot, BTREEGETNODE(rblock, half, keysize), BTREENODESIZE(keysize));
			llastnode->child = pivot.child;
			llastnode->num = pivot.num;
			//update parentnode for the left
			axisbtreenode pn = BTREEGETNODE(lblock, 0, keysize);
			for (int j = 0; j <= lblock->num; j++)
			{
				if (pn->child != 0) {
					FSEEK(btree->f, pn->child + sizeof(DATABASETYPE), SEEK_SET);
					FWRITE(&stack[left].seek, sizeof(DATABASETYPE), 1, btree->f);
				}
				pn = (axisbtreenode)(((char*)pn) + BTREENODESIZE(keysize));
			}
			//do right
			rblock->num = half;
			axisbtreenode rlastnode = BTREEGETNODE(rblock, half, keysize);
			CPYMEM(rnode, BTREEGETNODE(rblock, half + 1, keysize), BTREENODESIZE(keysize) * half);	//safe, no overlap
			//last node for right
			rlastnode->child = lastnode.child;							
			rlastnode->num = lastnode.num;
			rlastnode->data = 0;
			SETMEM(rlastnode->key, 0, keysize);
			//add pivot to parent
			levelindex--;
			block = stack[levelindex].block;
			//save last node before insert a pivot in parent
			plastnode = BTREEGETNODE(block, block->num, keysize);
			CPYMEM(&lastnode,plastnode,BTREENODESIZE(keysize));
			int i = block->num - 1;
			n = BTREEGETNODE(block, (i >= 0) ? i:0 , keysize);
			int c;
			while ((i>=0) && (c = BTREECOMPARE(c, pivot.key, pivot.data, (const char*)n->key, n->data, keysize) < 0)) {
				//shift
				axisbtreenode n2 = BTREEGETNODE(block, (i + 1), keysize);
				CPYMEM(n2, n, BTREENODESIZE(keysize));
				i--;
				n=BTREEGETNODE(block, (i >= 0) ? i : 0, keysize);
			}
			if (i < 0) { i = 0; } else i++;
			n = BTREEGETNODE(block,i, keysize);
			n->data = pivot.data;
			CPYMEM(n->key, pivot.key, keysize);
			n->child = stack[left].seek;
			//no more balancing up then copy lastnode
			if (block->num < numkey) {
				CPYMEM(BTREEGETNODE(block, block->num + 1, keysize),&lastnode, BTREENODESIZE(keysize));
			}
			//update numofchild 
			if ((btree->header.type & AXISBTREEPLUSTYPE) == AXISBTREEPLUSTYPE) {
				//left
				n->num = 0;	//sum
				for (int j = 0; j <= lblock->num; j++)
					n->num += BTREEGETNODE(lblock, j, keysize)->num + ((j != lblock->num) ? 1 : 0);
				//right (it can be last node)
				n = (i + 1 <= numkey) ? BTREEGETNODE(block, i + 1, keysize) : &lastnode;
				n->num = 0;	//sum
				for (int j = 0; j <= rblock->num; j++)
					n->num += BTREEGETNODE(rblock, j, keysize)->num + ((j != rblock->num) ? 1 : 0);
			}
			block->num++;
			//update right & left (parent later)
			FSEEK(btree->f, stack[right].seek, SEEK_SET);
			FWRITE(rblock, blocksize, 1, btree->f);
			FSEEK(btree->f, stack[left].seek, SEEK_SET);
			FWRITE(lblock, blocksize, 1, btree->f);
		}
	}
	//update parent here 
	FSEEK(btree->f, stack[levelindex].seek, SEEK_SET);
	FWRITE(block, blocksize, 1, btree->f);
	if ((btree->header.type & AXISBTREEPLUSTYPE)==AXISBTREEPLUSTYPE) btreeshift(btree,stack,levelindex,1);	
	btree->header.numnode++;
	return true;
}

FORCE_INLINE bool btreeuniquegrowup(axisbtree btree, struct btreestack* stack, int levelindex, const char* key, DATABASETYPE id, bool update)
{
	int keysize = btree->header.keysize;
	int numkey = btree->header.numkey;
	int maxlevel = btree->header.maxlevel;
	int blocksize = BTREEBLOCKSIZE(btree->header.keysize, btree->header.numkey);
	//simply add
	axisbtreeblock block = stack[levelindex].block;
	axisbtreenode plastnode = BTREEGETNODE(block, block->num, keysize);
	struct axisbtreenodeS lastnode;
	CPYMEM(&lastnode, plastnode, BTREENODESIZE(keysize));
	if (block->num != numkey) {		//copy the last node
		CPYMEM(BTREEGETNODE(block, block->num + 1, keysize), plastnode, BTREENODESIZE(keysize));
	}
	int c = -1;
	int i = block->num - 1;
	axisbtreenode n = BTREEGETNODE(block, (i >= 0) ? i : 0, keysize);
	while ((i >= 0) && ((memcmp(key, (const char*)n->key, keysize)) < 0)) {
		//shift
		axisbtreenode n2 = BTREEGETNODE(block, (i + 1), keysize);
		CPYMEM(n2, n, BTREENODESIZE(keysize));
		i--;
		n = BTREEGETNODE(block, (i >= 0) ? i : 0, keysize);
	}
	if (c == 0) {					//exists
		if (update) {
			FSEEK(btree->f, stack[levelindex].seek + BTREESEEKNODE(i, keysize) + 2 * sizeof(DATABASETYPE), SEEK_SET);
			FWRITE(&id, sizeof(DATABASETYPE), 1, btree->f);
		}
		return false;		
	}
	if (i >= 0) {					//not empty, get the space
		n = BTREEGETNODE(block, i + 1, keysize);
	}
	n->data = id;
	CPYMEM(n->key, key, keysize);
	n->child = n->num = 0;
	block->num++;
	while (block->num > numkey) {
		if ((levelindex == 0) || (levelindex+3<maxlevel)) {			//balance down(for root and level not equal)
			int left = levelindex + 1;
			int right = levelindex + 2;
			int half = block->num / 2;
			block->num = 1;
			axisbtreenode lnode = BTREEGETNODE(block, 0, keysize);		//first node
			axisbtreenode rnode = BTREEGETNODE(block, 1, keysize);		//last node
			axisbtreenode pnode = BTREEGETNODE(block, half, keysize);	//pivot node
			//do left 
			axisbtreeblock lblock = stack[left].block;
			lblock->parentnode = stack[levelindex].seek;
			lblock->num = btree->header.numkey - half;
			CPYMEM(BTREEGETNODE(lblock, 0, keysize), BTREEGETNODE(block, 0, keysize), BTREENODESIZE(keysize) * (lblock->num));
			//last node for left
			axisbtreenode llastnode = BTREEGETNODE(lblock, lblock->num, keysize);
			SETMEM(llastnode, 0, BTREENODESIZE(keysize));
			llastnode->child = pnode->child;
			llastnode->num = pnode->num;
			//do right
			axisbtreeblock rblock = stack[right].block;
			rblock->parentnode = stack[levelindex].seek;
			rblock->num = half;
			CPYMEM(BTREEGETNODE(rblock, 0, keysize), BTREEGETNODE(block, half + 1, keysize), BTREENODESIZE(keysize) * (rblock->num));
			//last node for right
			CPYMEM(BTREEGETNODE(rblock, rblock->num, keysize), &lastnode, BTREENODESIZE(keysize));
			//do parent (only 1 node)
			lnode->data = pnode->data;
			CPYMEM(lnode->key, pnode->key, keysize);
			lnode->child = BTREEHEADERSIZE + btree->header.numblock * blocksize;
			rnode->data = 0;
			SETMEM(rnode->key, 0, keysize);
			rnode->child = BTREEHEADERSIZE + (btree->header.numblock + 1) * blocksize;
			//update numofchild
			if ((btree->header.type & AXISBTREEPLUSTYPE) == AXISBTREEPLUSTYPE) {
				lnode->num = 0;
				for (int i = 0; i <= lblock->num; i++) {
					axisbtreenode node = BTREEGETNODE(lblock, i, keysize);
					lnode->num += node->num + ((i == lblock->num) ? 0 : 1);
				}
				rnode->num = 0;
				for (int i = 0; i <= rblock->num; i++) {
					axisbtreenode node = BTREEGETNODE(rblock, i, keysize);
					rnode->num += node->num + ((i == rblock->num) ? 0 : 1);
				}
			}
			//update parent later
			FSEEK(btree->f, lnode->child, SEEK_SET);
			FWRITE(lblock, blocksize, 1, btree->f);
			FSEEK(btree->f, rnode->child, SEEK_SET);
			FWRITE(rblock, blocksize, 1, btree->f);
			//update parentnode of childs (no need if insert-only btree)
			axisbtreenode n = BTREEGETNODE(lblock, 0, keysize);
			for (int i = 0; i <= lblock->num; i++)
			{
				if (n->child != 0) {
					FSEEK(btree->f, n->child + sizeof(DATABASETYPE), SEEK_SET);
					FWRITE(&lnode->child, sizeof(DATABASETYPE), 1, btree->f);
				}
				n = (axisbtreenode)(((char*)n) + BTREENODESIZE(keysize));
			}
			n = BTREEGETNODE(rblock, 0, keysize);
			for (int i = 0; i <= rblock->num; i++)
			{
				if (n->child != 0) {
					FSEEK(btree->f, n->child + sizeof(DATABASETYPE), SEEK_SET);
					FWRITE(&rnode->child, sizeof(DATABASETYPE), 1, btree->f);
				}
				n = (axisbtreenode)(((char*)n) + BTREENODESIZE(keysize));
			}
			btree->header.numblock += 2;					//always end
		}
		else {												//balance up
			int half = btree->header.numkey / 2;
			int left = levelindex + 1;
			int right = levelindex;
			//new node for the left
			stack[left].seek = BTREEHEADERSIZE + (btree->header.numblock * blocksize);
			btree->header.numblock++;
			axisbtreeblock lblock = stack[left].block;
			//previous node is right 
			axisbtreeblock rblock = stack[levelindex].block;
			//do left
			lblock->num = btree->header.numkey - half;
			lblock->parentnode = stack[levelindex - 1].seek;
			axisbtreenode lnode = BTREEGETNODE(lblock, 0, keysize);
			axisbtreenode rnode = BTREEGETNODE(rblock, 0, keysize);
			CPYMEM(lnode, rnode, BTREENODESIZE(keysize) * lblock->num);
			//last node for left
			axisbtreenode llastnode = BTREEGETNODE(lblock, lblock->num, keysize);
			SETMEM(llastnode, 0, BTREENODESIZE(keysize));
			//copy pivot
			struct axisbtreenodeS pivot;
			CPYMEM(&pivot, BTREEGETNODE(rblock, half, keysize), BTREENODESIZE(keysize));
			llastnode->child = pivot.child;
			llastnode->num = pivot.num;
			//update parentnode for the left
			axisbtreenode pn = BTREEGETNODE(lblock, 0, keysize);
			for (int j = 0; j <= lblock->num; j++)
			{
				if (pn->child != 0) {
					FSEEK(btree->f, pn->child + sizeof(DATABASETYPE), SEEK_SET);
					FWRITE(&stack[left].seek, sizeof(DATABASETYPE), 1, btree->f);
				}
				pn = (axisbtreenode)(((char*)pn) + BTREENODESIZE(keysize));
			}
			//do right
			rblock->num = half;
			axisbtreenode rlastnode = BTREEGETNODE(rblock, half, keysize);
			CPYMEM(rnode, BTREEGETNODE(rblock, half + 1, keysize), BTREENODESIZE(keysize) * half);	//safe, no overlap
			//last node for right
			rlastnode->child = lastnode.child;
			rlastnode->num = lastnode.num;
			rlastnode->data = 0;
			SETMEM(rlastnode->key, 0, keysize);
			//add pivot to parent
			levelindex--;
			block = stack[levelindex].block;
			//save last node before insert a pivot in parent
			plastnode = BTREEGETNODE(block, block->num, keysize);
			CPYMEM(&lastnode, plastnode, BTREENODESIZE(keysize));
			int i = block->num - 1;
			n = BTREEGETNODE(block, (i >= 0) ? i : 0, keysize);
			int c;
			while ((i >= 0) && (memcmp(pivot.key, (const char*)n->key, keysize) < 0)) {
				//shift
				axisbtreenode n2 = BTREEGETNODE(block, (i + 1), keysize);
				CPYMEM(n2, n, BTREENODESIZE(keysize));
				i--;
				n = BTREEGETNODE(block, (i >= 0) ? i : 0, keysize);
			}
			if (i < 0) { i = 0; }
			else i++;
			n = BTREEGETNODE(block, i, keysize);
			n->data = pivot.data;
			CPYMEM(n->key, pivot.key, keysize);
			n->child = stack[left].seek;
			//no more balancing up then copy lastnode
			if (block->num < numkey) {
				CPYMEM(BTREEGETNODE(block, block->num + 1, keysize), &lastnode, BTREENODESIZE(keysize));
			}
			//update numofchild 
			if ((btree->header.type & AXISBTREEPLUSTYPE) == AXISBTREEPLUSTYPE) {
				//left
				n->num = 0;	//sum
				for (int j = 0; j <= lblock->num; j++)
					n->num += BTREEGETNODE(lblock, j, keysize)->num + ((j != lblock->num) ? 1 : 0);
				//right (it can be last node)
				n = (i + 1 <= numkey) ? BTREEGETNODE(block, i + 1, keysize) : &lastnode;
				n->num = 0;	//sum
				for (int j = 0; j <= rblock->num; j++)
					n->num += BTREEGETNODE(rblock, j, keysize)->num + ((j != rblock->num) ? 1 : 0);
			}
			block->num++;
			//update right & left (parent later)
			FSEEK(btree->f, stack[right].seek, SEEK_SET);
			FWRITE(rblock, blocksize, 1, btree->f);
			FSEEK(btree->f, stack[left].seek, SEEK_SET);
			FWRITE(lblock, blocksize, 1, btree->f);
		}
	}
	//update parent here 
	FSEEK(btree->f, stack[levelindex].seek, SEEK_SET);
	FWRITE(block, blocksize, 1, btree->f);
	if ((btree->header.type & AXISBTREEPLUSTYPE) == AXISBTREEPLUSTYPE) btreeshift(btree, stack, levelindex, 1);
	btree->header.numnode++;
	if (levelindex > btree->header.maxlevel) btree->header.maxlevel = levelindex;
	return true;
}

FORCE_INLINE int btreeuniquebsearch(axisbtreeblock block, const char* key, int keysize, int* res)
{
	int nodesize = BTREENODESIZE(keysize);
	axisbtreenode node=NULL;
	int k = 0;
	int i = 0;
	int j = block->num - 1;
	int r = -1;
	while (r != 0) {
		if (i >= j) {
			int s = (j - 1 < 0) ? 0 : j - 1;
			int e = (i + 1 > block->num - 1) ? block->num - 1 : i + 1;
			for (; s <= e; s++) {
				node = BTREEGETNODE(block, s, keysize);
				int rr = memcmp(key, node->key, keysize);  
				if (rr == 0) { *res = s; return 2; }
				else if (rr < 0) { *res = s; return (node->child == 0) ? 0 : 1; }
			}
			*res = e + 1;
			return ((node==NULL) || (node->child == 0)) ? 0 : 1;
		}
		k = (i + j) / 2;
		node = BTREEGETNODE(block, k, keysize);
		r = memcmp(key, node->key, keysize);
		if (r == 0) { *res = k; return 2; }
		else if (r > 0) { i = k + 1; }
		else if (r < 0) { j = k - 1; }
	}
	*res = k;
	return (node->child == 0) ? 0 : 1;
}

FORCE_INLINE int btreeuniqueseqsearch(axisbtreeblock block, const char* key, int keysize, int* res)
{
	int nodesize = BTREENODESIZE(keysize);
	axisbtreenode node = BTREEGETNODE(block, 0, keysize);
	int c = 1;
	*res = 0;
	while ((*res < block->num) && ((c = memcmp(key,node->key, keysize)) > 0))
	{
		node = (axisbtreenode)(((char*)node) + nodesize);
		*res++;
	}
	if (c == 0) return 2;		//found
	return (node->child == 0) ? 0 : 1;
}

FORCE_INLINE int btreebsearch(axisbtreeblock block, const char* key, int keysize, DATABASETYPE id, int* res)
{
	int nodesize = BTREENODESIZE(keysize);
	axisbtreenode node=NULL;
	int k = 0;
	int i = 0;
	int j = block->num - 1;
	int r = -1;
	while (r != 0) {
		if (i >= j) {
			int s = (j - 1 < 0) ? 0 : j - 1;
			int e = (i + 1 > block->num - 1) ? block->num - 1 : i + 1;
			for (; s <= e; s++) {
				node = BTREEGETNODE(block, s, keysize);
				int rr = BTREECOMPARE(rr, key, id, node->key, node->data, keysize);
				if (rr == 0) { *res = s; return 2; }
				else if (rr < 0) { *res = s; return (node->child == 0) ? 0 : 1; }
			}
			*res = e + 1;	//greatest case
			node = BTREEGETNODE(block, e+1, keysize);
			return (node->child == 0) ? 0 : 1;
		}
		k = (i + j) / 2;
		node = BTREEGETNODE(block, k, keysize);
		r = BTREECOMPARE(r, key, id, node->key, node->data, keysize);
		if (r > 0) { i = k + 1; }
		else if (r < 0) { j = k - 1; }
	}
	*res = k;	//found
	return 2;
}

FORCE_INLINE int btreeseqsearch(axisbtreeblock block, const char* key, int keysize, DATABASETYPE id, int* res)
{
	int nodesize = BTREENODESIZE(keysize);
	axisbtreenode node = BTREEGETNODE(block, 0, keysize);
	int c = 1;
	*res = 0;
	while ((*res < block->num) && (c = BTREECOMPARE(c, key, id, node->key, node->data, keysize)) > 0)
	{
		node = (axisbtreenode)(((char*)node) + nodesize);
		(*res)++;
	}
	if (c == 0) return 2;		//found
	return (node->child == 0) ? 0 : 1;
}

FORCE_INLINE bool btreeisleaf(axisbtreeblock block,int keysize)
{
	axisbtreenode node = BTREEGETNODE(block, 0, keysize);
	for (int i = 0; i <= block->num; i++)
	{
		if (node->child != 0) return false;
		node = (axisbtreenode)(((char*)node) + BTREENODESIZE(keysize));
	}
	return true;
}

bool insertbtree(axisbtree btree, const char* key, DATABASETYPE id)
{
	int keysize = btree->header.keysize;
	int blocksize = BTREEBLOCKSIZE(btree->header.keysize, btree->header.numkey);
	//get root level
	FSEEK(btree->f, BTREEHEADERSIZE, SEEK_SET);
	FREAD(btree->block, blocksize, 1, btree->f);
	int maxlevel = btree->header.maxlevel;
	struct btreestack* stack= initbtreestack(btree->block,blocksize,maxlevel);
	int levelindex = 0;
	while (!btreeisleaf(stack[levelindex].block,keysize)) {
		axisbtreeblock block = stack[levelindex].block;
		int i = 0;
		int c = btreebsearch(block, key, keysize, id, &i);
		if (c == 2) {	//exists
			destroybtreestack(stack, maxlevel);
			return false;
		}
		axisbtreenode node = BTREEGETNODE(block, i, keysize);	
		if (node->child != 0) {
			stack[levelindex].i = i;
			levelindex++;
			stack[levelindex].seek = node->child;
			FSEEK(btree->f, node->child, SEEK_SET);
			FREAD(stack[levelindex].block, blocksize, 1, btree->f);
		}
		else {
			//add new child, then end. (used when delete)
			DATABASETYPE seek = stack[levelindex].seek;
			stack[levelindex].i = i;
			node->child = BTREEHEADERSIZE + btree->header.numblock * blocksize;			
			levelindex++;
			axisbtreeblock block = stack[levelindex].block;
			stack[levelindex].seek = node->child;
			SETMEM(block, 0, blocksize);
			block->num = 1;
			block->parentnode = stack[levelindex-1].seek;
			axisbtreenode n = BTREEGETNODE(block, 0, keysize);
			n->data = id;
			CPYMEM(n->key, key,keysize);
			//write new node
			FSEEK(btree->f, stack[levelindex].seek, SEEK_SET);
			FWRITE(block, blocksize, 1, btree->f);
			//update parent
			FSEEK(btree->f, seek + BTREESEEKNODE(i, keysize) + sizeof(DATABASETYPE), SEEK_SET);
			FWRITE(&node->child, sizeof(DATABASETYPE), 1, btree->f);
			//update header
			if ((btree->header.type & AXISBTREEPLUSTYPE)==AXISBTREEPLUSTYPE) btreeshift(btree, stack, levelindex,1);
			btree->header.numnode++;
			if (levelindex > btree->header.maxlevel) btree->header.maxlevel = levelindex;
			btree->header.numblock++;
			destroybtreestack(stack,maxlevel);
			return true;
		}
	}
	axisbtreeblock block = stack[levelindex].block;
	bool ret=btreegrowup(btree,stack, levelindex, key, id);
	destroybtreestack(stack,maxlevel);
	return ret;
}

bool insertuniquebtree(axisbtree btree, const char* key, DATABASETYPE id,bool update)
{	
	int keysize = btree->header.keysize;
	int blocksize = BTREEBLOCKSIZE(btree->header.keysize, btree->header.numkey);
	//get root level
	FSEEK(btree->f, BTREEHEADERSIZE, SEEK_SET);
	FREAD(btree->block, blocksize, 1, btree->f);
	int maxlevel = btree->header.maxlevel;
	struct btreestack* stack = initbtreestack(btree->block, blocksize, maxlevel);
	int levelindex = 0;
	while (!btreeisleaf(stack[levelindex].block, keysize)) {
		axisbtreeblock block = stack[levelindex].block;
		int i = 0;
		int c = btreeuniquebsearch(block, key, keysize, &i);
		if (c == 2) {	//exists
			if (update) {
				FSEEK(btree->f, stack[levelindex].seek + BTREESEEKNODE(i, keysize) + 2 * sizeof(DATABASETYPE), SEEK_SET);
				FWRITE(&id, sizeof(DATABASETYPE), 1, btree->f);
			}
			destroybtreestack(stack, maxlevel);
			return false;
		}
		axisbtreenode node = BTREEGETNODE(block, i, keysize);
		if (node->child != 0) {
			stack[levelindex].i = i;
			levelindex++;
			stack[levelindex].seek = node->child;
			FSEEK(btree->f, node->child, SEEK_SET);
			FREAD(stack[levelindex].block, blocksize, 1, btree->f);
		}
		else {
			//add new child, then end. (used when delete)
			DATABASETYPE seek = stack[levelindex].seek;
			stack[levelindex].i = i;
			node->child = BTREEHEADERSIZE + btree->header.numblock * blocksize;
			levelindex++;
			axisbtreeblock block = stack[levelindex].block;
			stack[levelindex].seek = node->child;
			SETMEM(block, 0, blocksize);
			block->num = 1;
			block->parentnode = stack[levelindex - 1].seek;
			axisbtreenode n = BTREEGETNODE(block, 0, keysize);
			n->data = id;
			CPYMEM(n->key, key, keysize);
			//write new node
			FSEEK(btree->f, stack[levelindex].seek, SEEK_SET);
			FWRITE(block, blocksize, 1, btree->f);
			//update parent
			FSEEK(btree->f, seek + BTREESEEKNODE(i, keysize) + sizeof(DATABASETYPE), SEEK_SET);
			FWRITE(&node->child, sizeof(DATABASETYPE), 1, btree->f);
			//update header
			if ((btree->header.type & AXISBTREEPLUSTYPE) == AXISBTREEPLUSTYPE) btreeshift(btree, stack, levelindex, 1);
			btree->header.numnode++;
			if (levelindex > btree->header.maxlevel) btree->header.maxlevel = levelindex;
			btree->header.numblock++;
			destroybtreestack(stack, maxlevel);
			return true;
		}
	}
	axisbtreeblock block = stack[levelindex].block;
	bool ret = btreeuniquegrowup(btree, stack, levelindex, key, id, update);
	destroybtreestack(stack, maxlevel);
	return ret;
}

FORCE_INLINE void btreeremoveblock(axisbtree btree, DATABASETYPE seek)
{
	unsigned short keysize = btree->header.keysize;
	int blocksize = BTREEBLOCKSIZE(keysize, btree->header.numkey);
	btree->header.numblock--;
	DATABASETYPE lastseek = BTREEHEADERSIZE + (btree->header.numblock) * blocksize;
	if (lastseek > seek) {
		//move
		FSEEK(btree->f, lastseek, SEEK_SET);
		FREAD(btree->block, blocksize, 1, btree->f);
		DATABASETYPE parentseek = btree->block->parentnode;
		FSEEK(btree->f, seek, SEEK_SET);
		FWRITE(btree->block, blocksize, 1, btree->f);
		//update parentnode of child
		axisbtreenode n = BTREEGETNODE(btree->block, 0, keysize);
		for (int j = 0; j <= btree->block->num; j++)
		{
			if (n->child != 0) {
				FSEEK(btree->f, n->child + sizeof(DATABASETYPE), SEEK_SET);
				FWRITE(&seek, sizeof(DATABASETYPE), 1, btree->f);
			}
			n = (axisbtreenode)(((char*)n) + BTREENODESIZE(keysize));
		}
		//update parent
		FSEEK(btree->f, parentseek, SEEK_SET);
		FREAD(btree->block, blocksize, 1, btree->f);
		for (int i = 0; i <= btree->block->num; i++)
		{
			axisbtreenode node = BTREEGETNODE(btree->block, i, keysize);
			if (node->child == lastseek) {	//TODO: optimized only parent
				node->child = seek;
			}
		}
		FSEEK(btree->f, parentseek, SEEK_SET);
		FWRITE(btree->block, blocksize, 1, btree->f);
	}
}

FORCE_INLINE bool btreefalldown(axisbtree btree, struct btreestack* stack, int levelindex)
{	//levelindex is where node deleted
	int numlevelindex = levelindex;
	int newlevelindex = levelindex + 1;
	int numkey = btree->header.numkey;
	int keysize = btree->header.keysize;
	int blocksize = BTREEBLOCKSIZE(keysize, numkey);
	bool end = false;
	while ((levelindex > 0) && (!end)) {
		axisbtreeblock block = stack[levelindex].block;
		DATABASETYPE seek = stack[levelindex].seek;
		int pnum = block->num;	//p at current level
		levelindex--;			//up 1 level
		int pi = stack[levelindex].i;
		axisbtreeblock parentblock = stack[levelindex].block;
		axisbtreenode pnode = BTREEGETNODE(parentblock, pi, keysize);
		axisbtreenode lnode = (pi == 0) ? NULL : BTREEGETNODE(parentblock, pi - 1, keysize);
		axisbtreenode rnode = (pi == parentblock->num) ? NULL : BTREEGETNODE(parentblock, pi + 1, keysize);
		DATABASETYPE lnum = numkey; //maxmimum
		DATABASETYPE rnum = numkey;
		if ((lnode != NULL) && (lnode->child != 0)) {
			FSEEK(btree->f, lnode->child, SEEK_SET);
			FREAD(&lnum, sizeof(DATABASETYPE), 1, btree->f);
		}
		if ((rnode != NULL) && (rnode->child != 0)) {
			FSEEK(btree->f, rnode->child, SEEK_SET);
			FREAD(&rnum, sizeof(DATABASETYPE), 1, btree->f);
		}
		int mergeside = 0;
		if ((lnum + 1 + pnum <= numkey) && (pnum + 1 + rnum <= numkey)) {
			mergeside = (lnum > rnum) ? 1 : 2;
		} else if (lnum + 1 + pnum <= numkey) {
			mergeside = 1;
		} else if (pnum + 1 + rnum <= numkey) {
			mergeside = 2; 
		} 		
		if (mergeside == 1) {					//merge left, pivot is lnode, remove lnode, remove block from the left, set child to pnode
			FSEEK(btree->f,lnode->child,SEEK_SET);
			FREAD(stack[newlevelindex].block, blocksize, 1, btree->f);
			axisbtreenode n = BTREEGETNODE(block, block->num , keysize);
			axisbtreenode nn = BTREEGETNODE(block, block->num +lnum+1, keysize);
			//do right
			for (int j = block->num; j >=0; j--)		//shift 
			{
				CPYMEM(nn, n,BTREENODESIZE(keysize));
				n = (axisbtreenode)(((char*)n) - BTREENODESIZE(keysize));
				nn = (axisbtreenode)(((char*)nn) - BTREENODESIZE(keysize));
			}
			//do left
			axisbtreeblock lblock = stack[newlevelindex].block;
			CPYMEM(BTREEGETNODE(block, 0, keysize), BTREEGETNODE(lblock, 0, keysize), BTREENODESIZE(keysize)*lnum);
			//clear left node first
			axisbtreenode lastnode = BTREEGETNODE(lblock, lblock->num, keysize);
			block->num = lnum + 1 + pnum;
			stack[levelindex].rmseek = lnode->child;
			//merge pivot (lnode) with the child from last node of the left
			lnode->child = lastnode->child;
			lnode->num = lastnode->num;
			CPYMEM(BTREEGETNODE(block, lnum, keysize), lnode, BTREENODESIZE(keysize));
			//update parentnode of the left (including lastnode)
			n = BTREEGETNODE(block, 0, keysize);
			for (int j = 0; j <= lnum; j++)
			{
				if (n->child != 0) {
					FSEEK(btree->f, n->child + sizeof(DATABASETYPE), SEEK_SET);
					FWRITE(&seek, sizeof(DATABASETYPE), 1, btree->f);
				}
				n = (axisbtreenode)(((char*)n) + BTREENODESIZE(keysize));
			}			
			//fix parent (remove lnode)
			n = BTREEGETNODE(parentblock, pi - 1, keysize);
			for (int j = pi-1; j < parentblock->num; j++)		//shift 
			{
				CPYMEM(n, ((char*)n)+BTREENODESIZE(keysize), BTREENODESIZE(keysize));
				n= (axisbtreenode)(((char*)n) + BTREENODESIZE(keysize));
			}
			stack[levelindex].i--;			//shift 
			parentblock->num--;
		} else if (mergeside == 2) {		//merge right, pivot is pnode, remove pnode, remove block from the right, set child to rnode
			FSEEK(btree->f, rnode->child, SEEK_SET);
			FREAD(stack[newlevelindex].block, blocksize, 1, btree->f);
			//no need for the left
			//merge pivot (pnode) with the child from last node of the left
			axisbtreenode pivot = BTREEGETNODE(block, block->num, keysize);
			CPYMEM(pivot->key , pnode->key,keysize);
			pivot->data = pnode->data;
			//do right
			axisbtreeblock rblock = stack[newlevelindex].block;
			CPYMEM(BTREEGETNODE(block, block->num + 1, keysize), BTREEGETNODE(rblock,0,keysize),(rblock->num+1)*BTREENODESIZE(keysize));			
			//update parentnode of the right
			axisbtreenode n = BTREEGETNODE(rblock, 0, keysize);
			for (int j = 0; j <= rblock->num; j++)
			{
				if (n->child != 0) {
					FSEEK(btree->f, n->child + sizeof(DATABASETYPE), SEEK_SET);
					FWRITE(&seek, sizeof(DATABASETYPE), 1, btree->f);
				}
				n = (axisbtreenode)(((char*)n) + BTREENODESIZE(keysize));
			}
			block->num = pnum + 1 + rnum;
			stack[levelindex].rmseek = rnode->child;
			//fix parent (copy child from pnode, remove pnode)
			n = BTREEGETNODE(parentblock, pi , keysize);
			DATABASETYPE child = n->child;
			for (int j = pi ; j < parentblock->num; j++)		//shift 
			{
				CPYMEM(n, ((char*)n) + BTREENODESIZE(keysize), BTREENODESIZE(keysize));
				n = (axisbtreenode)(((char*)n) + BTREENODESIZE(keysize));
			}
			//fix parent (set child to rnode)
			n = BTREEGETNODE(parentblock, pi, keysize);
			n->child = child;
			parentblock->num--;
		}
		else {
			//update seek to next child if child->num ==0 
			//2 case 
			// - getmin->copy numofchild from lastnode
			// - at leaf (in stack) -> skip stck to get num
			if (pnum==0)  {
				axisbtreenode n=BTREEGETNODE(stack[levelindex].block,stack[levelindex].i,keysize);
				if (numlevelindex >= levelindex + 2) {
					axisbtreeblock cblock = stack[levelindex + 2].block;
					n->child = stack[levelindex+2].seek;
					n->num = cblock->num;
					for (int k = 0; k <= cblock->num; k++) 
						n->num += BTREEGETNODE(cblock, k, keysize)->num;
					cblock->parentnode = stack[levelindex].seek;
				}
				else {										//not in stack just copy, and set parent
					axisbtreeblock cblock = stack[levelindex + 1].block;
					DATABASETYPE pchild = n->child;
					axisbtreenode nnode = BTREEGETNODE(cblock, 0, keysize);
					DATABASETYPE nchild = nnode->child;
					n->child = nchild;													//next child
					n->num = nnode->num;
					FSEEK(btree->f, nchild + sizeof(DATABASETYPE), SEEK_SET);
					FWRITE(&stack[levelindex].seek, sizeof(DATABASETYPE), 1, btree->f);	//set parent
				}
			}
			end=true;	//end 
		}
	}
	//correct number
	DATABASETYPE accnum = 0;
	for (int j = numlevelindex-1; j >= 0; j--)
	{
		axisbtreeblock block = stack[j].block;
		if (block->num > 0) {
			axisbtreenode n = BTREEGETNODE(stack[j].block, stack[j].i, keysize);
			if (n->child == stack[j+1].seek) {
				axisbtreeblock cblock=stack[j+1].block;
				n->num = cblock->num;
				for (int k = 0; k <= cblock->num; k++)
				{
					n->num += BTREEGETNODE(cblock, k, keysize)->num;
				}
			}
		}
	}
	//correct the root 
	if (stack[0].block->num==0) {			
		if (numlevelindex > 0) {	//special case
			CPYMEM(stack[0].block, stack[1].block, blocksize);
			stack[0].i = stack[1].i;
			stack[0].block->parentnode = 0;
			stack[1].block->num = 0;
			//update parent
			axisbtreenode n = BTREEGETNODE(stack[0].block, 0, keysize);
			int num = stack[0].block->num;
			for (int j = 0; j <= num; j++)
			{
				if (n->child != 0) {
					FSEEK(btree->f, n->child + sizeof(DATABASETYPE), SEEK_SET);
					FWRITE(&stack[0].seek, sizeof(DATABASETYPE), 1, btree->f);
				}
				n = (axisbtreenode)(((char*)n) + BTREENODESIZE(keysize));
			}
			if (numlevelindex > 1) {
				if (stack[2].block->parentnode == stack[1].seek) 
					stack[2].block->parentnode = stack[0].seek;
			}
		}							//empty case do nothing
	}
	//write result first
	for (int i = levelindex; i <= numlevelindex; i++)
	if ((stack[i].block->num!=0) || (i==0)) {
		FSEEK(btree->f, stack[i].seek, SEEK_SET);
		FWRITE(stack[i].block,blocksize,1, btree->f);
	}
	//update numofchild 
	if ((btree->header.type & AXISBTREEPLUSTYPE) == AXISBTREEPLUSTYPE) {
		for (int i = 0; i < levelindex; i++)
			if (stack[i].block->num != 0)
			{
				FSEEK(btree->f, stack[i].seek+BTREESEEKNODE(stack[i].i,keysize), SEEK_SET);
				DATABASETYPE num = BTREEGETNODE(stack[i].block, stack[i].i, keysize)->num;
				FWRITE(&num, sizeof(DATABASETYPE), 1, btree->f);
			}
	}
	//remove deleted block (don't forget to sort the list first)
	int numdeleted = 0;
	DATABASETYPE* deletedlist = (DATABASETYPE*)ALLOCMEM(sizeof(DATABASETYPE)*numlevelindex*2);
	for (int i=0;i<=numlevelindex;i++)
	{
#define BTREE_DELETE()	if (seek > 0) {\
			deletedlist[numdeleted] = seek;\
			for (int k = numdeleted - 1; k >= 0; k--)\
			{\
				if (deletedlist[k] < seek) {\
					deletedlist[k + 1] = deletedlist[k];\
					deletedlist[k] = seek;\
				}\
				else break;\
			}\
			numdeleted++;\
		}
		DATABASETYPE seek = (stack[i].rmseek > 0)?stack[i].rmseek:0;
		BTREE_DELETE();
		seek = ((stack[i].block->num == 0) && (i != 0)) ? stack[i].seek : 0;
		BTREE_DELETE();
	}
	for (int i = 0; i < numdeleted; i++)
	{
		btreeremoveblock(btree, deletedlist[i]);
	}
	FREEMEM(deletedlist);
	return true;
}

FORCE_INLINE int btreegetmin(axisbtree btree, struct btreestack* stack, int levelindex, int i)
{   //get the least key then swap, may be remove node, i is the node to be delete. i+1 is the node to get min
	//return level index
	unsigned short keysize = btree->header.keysize;
	int blocksize = BTREEBLOCKSIZE(btree->header.keysize, btree->header.numkey);
	int currentlevelindex = levelindex;
	//goto child to find min
	axisbtreenode node = BTREEGETNODE(stack[levelindex].block, i, keysize);
	axisbtreenode rnode = BTREEGETNODE(stack[levelindex].block, i+1, keysize);
	stack[levelindex].i = i+1;	//change i
	FSEEK(btree->f, rnode->child, SEEK_SET);
	levelindex++;
	stack[levelindex].rmseek = 0;
	stack[levelindex].seek = rnode->child;
	FREAD(stack[levelindex].block, blocksize, 1, btree->f);
	bool found = false;
	while (!found) {
		axisbtreeblock block = stack[levelindex].block;
		stack[levelindex].i = 0;	//always first node
		axisbtreenode firstnode = BTREEGETNODE(block, 0, keysize);
		if (firstnode->child > 0) {
			FSEEK(btree->f, firstnode->child, SEEK_SET);
			levelindex++;
			stack[levelindex].seek = firstnode->child;
			stack[levelindex].rmseek = 0;
			FREAD(stack[levelindex].block, blocksize, 1, btree->f);
		}
		else {
			found = true;
			CPYMEM(node->key, firstnode->key, keysize);
			node->data = firstnode->data;
			//update deleted node first 
			FSEEK(btree->f, stack[currentlevelindex].seek + BTREESEEKNODE(i, keysize), SEEK_SET);
			FWRITE(node, BTREENODESIZE(keysize), 1, btree->f);
			axisbtreenode n = BTREEGETNODE(block, 0, keysize);
			int nodesize = BTREENODESIZE(keysize);
			for (int i=0;i<block->num;i++)	//shift
			{
				axisbtreenode nn = (axisbtreenode)(((char*)n)+nodesize);
				CPYMEM(n,nn,nodesize);
				n = nn;
			}
			block->num--;
			//remove an empty block later
		}
	}
	return levelindex;
}

FORCE_INLINE int btreegetmax(axisbtree btree, struct btreestack* stack, int levelindex, int i)
{	//get the greatest key then swap, may be remove node, i is the node to be delete.
	//return level index
	unsigned short keysize = btree->header.keysize;
	int blocksize = BTREEBLOCKSIZE(btree->header.keysize, btree->header.numkey);
	int currentlevelindex = levelindex;
	//goto child to find max
	axisbtreenode node = BTREEGETNODE(stack[levelindex].block, i, keysize);
	FSEEK(btree->f, node->child, SEEK_SET);
	levelindex++;
	stack[levelindex].rmseek = 0;
	stack[levelindex].seek = node->child;
	FREAD(stack[levelindex].block, blocksize, 1, btree->f);
	bool found = false;
	while (!found) {
		axisbtreeblock block = stack[levelindex].block;
		stack[levelindex].i = block->num;	//always lastnode
		axisbtreenode lastnode = BTREEGETNODE(block, block->num, keysize);
		if (lastnode->child > 0) {
			FSEEK(btree->f, lastnode->child, SEEK_SET);
			levelindex++;
			stack[levelindex].seek = lastnode->child;
			stack[levelindex].rmseek = 0;
			FREAD(stack[levelindex].block, blocksize, 1, btree->f);
		}
		else {
			found = true;
			axisbtreenode n= BTREEGETNODE(block, block->num-1, keysize);
			CPYMEM(node->key, n->key, keysize);
			node->data = n->data;
			//update deleted node first (may be no btreeshift)
			FSEEK(btree->f, stack[currentlevelindex].seek + BTREESEEKNODE(i, keysize), SEEK_SET);
			FWRITE(node, BTREENODESIZE(keysize), 1, btree->f);
			SETMEM(n->key, 0, keysize);
			n->data = 0;
			block->num--;
			//remove an empty block later
		}
	}
	return levelindex;
}

bool deletebtree(axisbtree btree, const char* key, DATABASETYPE id)
{
	int keysize = btree->header.keysize;
	int numkey = btree->header.numkey;
	int blocksize = BTREEBLOCKSIZE(btree->header.keysize, btree->header.numkey);
	DATABASETYPE rmseek = 0;
	//get root level
	FSEEK(btree->f, BTREEHEADERSIZE, SEEK_SET);
	FREAD(btree->block, blocksize, 1, btree->f);
	int rootlevel = btree->header.maxlevel;
	struct btreestack* stack = initbtreestack(btree->block, blocksize,rootlevel);
	int levelindex = 0;
	axisbtreeblock block = stack[levelindex].block;
	stack[levelindex].rmseek = 0;
	int i,c;
	while ((c = btreebsearch(block, key, keysize, id, &i)) == 1) {
		axisbtreenode node = BTREEGETNODE(block, i, keysize);
		stack[levelindex].i = i;
		levelindex++;
		stack[levelindex].seek = node->child;
		stack[levelindex].rmseek = 0;
		FSEEK(btree->f, node->child, SEEK_SET);
		FREAD(stack[levelindex].block, blocksize, 1, btree->f);
		block = stack[levelindex].block;
	}
	if (c==2) {				//found
		stack[levelindex].i = i;
		axisbtreenode node = BTREEGETNODE(block, i, keysize);
		axisbtreenode next = BTREEGETNODE(block, (i+1), keysize);
		if ((node->child == 0) && (next->child == 0)) {
			//simple remove
			axisbtreenode n = BTREEGETNODE(block, i, keysize);
			int nodesize = BTREENODESIZE(keysize);
			for (int j = i; j < block->num; j++)	//shift
			{
				axisbtreenode n2 = (axisbtreenode)(((char*)n) + nodesize);
				CPYMEM(n,n2, nodesize);
				n = n2;
			}
			block->num--;
		} else if (node->child == 0) {
			levelindex=btreegetmin(btree, stack, levelindex, i);
		} else if (next->child==0) {
			levelindex=btreegetmax(btree, stack, levelindex, i);
		} else {	//heuristic, choose greater between left or right
			DATABASETYPE lnum, rnum;
			FSEEK(btree->f,node->child,SEEK_SET);
			FREAD(&lnum,sizeof(DATABASETYPE),1,btree->f);
			FSEEK(btree->f,next->child,SEEK_SET);
			FREAD(&rnum,sizeof(DATABASETYPE),1,btree->f);
			levelindex = (lnum > rnum) ? btreegetmax(btree, stack, levelindex, i) : levelindex = btreegetmin(btree, stack, levelindex, i);
		}
		btreefalldown(btree, stack, levelindex);
	}
	destroybtreestack(stack,rootlevel);
	if (c == 2) btree->header.numnode--;
	return (c==2);
}

bool deleteuniquebtree(axisbtree btree, const char* key)
{
	int keysize = btree->header.keysize;
	int numkey = btree->header.numkey;
	int blocksize = BTREEBLOCKSIZE(btree->header.keysize, btree->header.numkey);
	DATABASETYPE rmseek = 0;
	//get root level
	FSEEK(btree->f, BTREEHEADERSIZE, SEEK_SET);
	FREAD(btree->block, blocksize, 1, btree->f);
	int rootlevel = btree->header.maxlevel;
	struct btreestack* stack = initbtreestack(btree->block, blocksize, rootlevel);
	int levelindex = 0;
	axisbtreeblock block = stack[levelindex].block;
	stack[levelindex].rmseek = 0;
	int i, c;
	while ((c = btreeuniquebsearch(block, key, keysize, &i)) == 1) {
		axisbtreenode node = BTREEGETNODE(block, i, keysize);
		stack[levelindex].i = i;
		levelindex++;
		stack[levelindex].seek = node->child;
		stack[levelindex].rmseek = 0;
		FSEEK(btree->f, node->child, SEEK_SET);
		FREAD(stack[levelindex].block, blocksize, 1, btree->f);
		block = stack[levelindex].block;
	}
	if (c == 2) {				//found
		stack[levelindex].i = i;
		axisbtreenode node = BTREEGETNODE(block, i, keysize);
		axisbtreenode next = BTREEGETNODE(block, (i + 1), keysize);
		if ((node->child == 0) && (next->child == 0)) {
			//simple remove
			axisbtreenode n = BTREEGETNODE(block, i, keysize);
			int nodesize = BTREENODESIZE(keysize);
			for (int j = i; j < block->num; j++)	//shift
			{
				axisbtreenode n2 = (axisbtreenode)(((char*)n) + nodesize);
				CPYMEM(n, n2, nodesize);
				n = n2;
			}
			block->num--;
		}
		else if (node->child == 0) {
			levelindex = btreegetmin(btree, stack, levelindex, i);
		}
		else if (next->child == 0) {
			levelindex = btreegetmax(btree, stack, levelindex, i);
		}
		else {	//heuristic, choose greater between left or right
			DATABASETYPE lnum, rnum;
			FSEEK(btree->f, node->child, SEEK_SET);
			FREAD(&lnum, sizeof(DATABASETYPE), 1, btree->f);
			FSEEK(btree->f, next->child, SEEK_SET);
			FREAD(&rnum, sizeof(DATABASETYPE), 1, btree->f);
			levelindex = (lnum > rnum) ? btreegetmax(btree, stack, levelindex, i) : levelindex = btreegetmin(btree, stack, levelindex, i);
		}
		btreefalldown(btree, stack, levelindex);
	}
	destroybtreestack(stack, rootlevel);
	if (c == 2) btree->header.numnode--;
	return (c == 2);
}

DATABASETYPE selectbtree(axisbtree btree, const char* key, DATABASETYPE offset, DATABASETYPE limit, DATABASETYPE* value)
{
	int ret = 0;
	int keysize = btree->header.keysize;
	int blocksize = BTREEBLOCKSIZE(btree->header.keysize, btree->header.numkey);
	//get root level
	FSEEK(btree->f, BTREEHEADERSIZE, SEEK_SET);
	FREAD(btree->block, blocksize, 1, btree->f);
	int rootlevel = btree->header.maxlevel;
	int levelindex = 0;
	struct btreestack* stack = initbtreestack(btree->block, blocksize, rootlevel);
	while ((levelindex>=0) && (ret < offset+limit)) {
		axisbtreeblock block = stack[levelindex].block;
		bool end = false;
		while ((!end) && (stack[levelindex].i <= block->num))	//= block->num for including last node
		{
			axisbtreenode node = BTREEGETNODE(block, stack[levelindex].i, keysize);
			int r = (stack[levelindex].i == block->num)?-2:memcmp(key,node->key, keysize);
			if (r < 0) {						//including last node
				if ((node->child > 0) && ((ret==0) || (r==-2))) {
					levelindex++;
					FSEEK(btree->f, node->child, SEEK_SET);
					FREAD(stack[levelindex].block, blocksize, 1, btree->f);
					stack[levelindex].i = 0;
					block = stack[levelindex].block;
				}
				else end = true;
			}
			else if (r == 0) {
				if (node->child > 0) {
					//optimize
					if ((ret > 0) && (node->num!=0) && (ret+node->num+1<offset)) {
						ret += node->num+1;
						stack[levelindex].i++;
					} else {
						levelindex++;
						FSEEK(btree->f, node->child, SEEK_SET);
						FREAD(stack[levelindex].block, blocksize, 1, btree->f);
						stack[levelindex].i = 0;
						stack[levelindex].seek = node->child;
						block = stack[levelindex].block;
					}
				}
				else {
					//select
					if (ret < offset + limit) {
						if (ret >= offset) value[ret - offset] = node->data;
						ret++;
					}
					else end = true;
					stack[levelindex].i++;
				}
			} else 
				stack[levelindex].i++;
		}
		levelindex--;
		if (levelindex >= 0) {		//select the parent node too
			end = false;
			axisbtreenode node = BTREEGETNODE(stack[levelindex].block, stack[levelindex].i, keysize);
			if (memcmp(key,node->key, keysize) == 0) {		//select
				if (ret < offset + limit) {
					if (ret >= offset)  value[ret - offset] = node->data;
					ret++;
				}
			}
			stack[levelindex].i++;
		}
	}
	destroybtreestack(stack, rootlevel);
	return (ret>offset)?ret-offset:0;
}

DATABASETYPE selectrangebtree(axisbtree btree, const char* keyfrom, const char* keyto, DATABASETYPE offset, DATABASETYPE limit, DATABASETYPE* value)
{
	int ret = 0;
	int keysize = btree->header.keysize;
	int blocksize = BTREEBLOCKSIZE(btree->header.keysize, btree->header.numkey);
	//get root level
	FSEEK(btree->f, BTREEHEADERSIZE, SEEK_SET);
	FREAD(btree->block, blocksize, 1, btree->f);
	int rootlevel = btree->header.maxlevel;
	int levelindex = 0;
	struct btreestack* stack = initbtreestack(btree->block, blocksize, rootlevel);
	while ((levelindex >= 0) && (ret < offset + limit)) {
		axisbtreeblock block = stack[levelindex].block;
		bool end = false;
		while ((!end) && (stack[levelindex].i <= block->num))	//= block->num for including last node
		{
			axisbtreenode node = BTREEGETNODE(block, stack[levelindex].i, keysize);
			int r = (stack[levelindex].i == block->num) ? -2 : memcmp(keyfrom, node->key, keysize);
			if (r <= 0) {						//including last node
				if (node->child > 0) {
					//optimized
					if ((stack[levelindex].i != block->num) && (memcmp(keyto, node->key, keysize) >= 0) && (ret > 0) && (node->num != 0) && (ret + node->num + 1 < offset))
					{
						ret += node->num + 1;
						stack[levelindex].i++;
					}
					else {
						levelindex++;
						FSEEK(btree->f, node->child, SEEK_SET);
						FREAD(stack[levelindex].block, blocksize, 1, btree->f);
						stack[levelindex].i = 0;
						block = stack[levelindex].block;
					}
				}
				else {
					if ((stack[levelindex].i != block->num) && (memcmp(keyto, node->key, keysize) >= 0)) {		//select
						if (ret < offset + limit) {
							if (ret >= offset)  value[ret - offset] = node->data;
							ret++;
						}
						stack[levelindex].i++;
					}
					else end = true;
				}
			}
			else
				stack[levelindex].i++;
		}
		levelindex--;
		if (levelindex >= 0) {		//select the parent node too
			end = false;
			axisbtreenode node = BTREEGETNODE(stack[levelindex].block, stack[levelindex].i, keysize);
			if ((stack[levelindex].i != stack[levelindex].block->num) && (memcmp(keyto, node->key, keysize) >= 0)) {		//select
				if (ret < offset + limit) {
					if (ret >= offset)  value[ret - offset] = node->data;
					ret++;
				}
			}
			stack[levelindex].i++;
		}
	}
	destroybtreestack(stack, rootlevel);
	return (ret > offset) ? ret - offset : 0;
}

DATABASETYPE countrangebtree(axisbtree btree, const char* keyfrom,const char* keyto)
{
	int ret = 0;
	int keysize = btree->header.keysize;
	int blocksize = BTREEBLOCKSIZE(btree->header.keysize, btree->header.numkey);
	//get root level
	FSEEK(btree->f, BTREEHEADERSIZE, SEEK_SET);
	FREAD(btree->block, blocksize, 1, btree->f);
	int rootlevel = btree->header.maxlevel;
	int levelindex = 0;
	struct btreestack* stack = initbtreestack(btree->block, blocksize, rootlevel);
	while (levelindex >= 0) {
		axisbtreeblock block = stack[levelindex].block;
		bool end = false;
		while ((!end) && (stack[levelindex].i <= block->num))	//= block->num for including last node
		{
			axisbtreenode node = BTREEGETNODE(block, stack[levelindex].i, keysize);
			int r = (stack[levelindex].i == block->num) ? -2 : memcmp(keyfrom, node->key, keysize);
			if (r <= 0) {						//including last node
				if (node->child > 0) {
					//optimized
					r = (stack[levelindex].i == block->num) ? -2 : memcmp(keyto, node->key, keysize);
					if ((r >= 0) && (node->num != 0) && (ret > 0))
					{
						ret += node->num+1;
						stack[levelindex].i++;
					}
					else {
						levelindex++;
						FSEEK(btree->f, node->child, SEEK_SET);
						FREAD(stack[levelindex].block, blocksize, 1, btree->f);
						stack[levelindex].i = 0;
						block = stack[levelindex].block;
					}
				}
				else {
					r = (stack[levelindex].i == block->num) ? -2 : memcmp(keyto, node->key, keysize);
					if (r>=0) {		//select
						ret++;
						stack[levelindex].i++;
					}
					else end = true;
				}
			}
			else
				stack[levelindex].i++;
		}
		levelindex--;
		if (levelindex >= 0) {		//select the parent node too
			end = false;
			axisbtreenode node = BTREEGETNODE(stack[levelindex].block, stack[levelindex].i, keysize);
			if ((stack[levelindex].i != stack[levelindex].block->num) && (memcmp(keyto, node->key, keysize) >= 0)) {		//select
				ret++;
			}
			stack[levelindex].i++;
		}
	}
	destroybtreestack(stack, rootlevel);
	return ret;
}

bool selectuniquebtree(axisbtree btree, const char* key,DATABASETYPE* ret)
{
	int keysize = btree->header.keysize;
	int blocksize = BTREEBLOCKSIZE(btree->header.keysize, btree->header.numkey);
	FSEEK(btree->f, BTREEHEADERSIZE, SEEK_SET);
	FREAD(btree->block, blocksize, 1, btree->f);
	int i,c;
	while ((c=btreeuniquebsearch(btree->block,key,keysize,&i))==1) {
		axisbtreenode node = BTREEGETNODE(btree->block, i, keysize);
		FSEEK(btree->f, node->child, SEEK_SET);
		FREAD(btree->block, blocksize, 1, btree->f);
	}
	if (c == 2) {
		*ret=BTREEGETNODE(btree->block, i, keysize)->data;
		return true;
	}
	return false;
}

DATABASETYPE selectuniquerangebtree(axisbtree btree, const char* keyfrom, const char* keyto, DATABASETYPE offset, DATABASETYPE limit, DATABASETYPE* value)
{
	int ret = 0;
	int keysize = btree->header.keysize;
	int blocksize = BTREEBLOCKSIZE(btree->header.keysize, btree->header.numkey);
	//get root level
	FSEEK(btree->f, BTREEHEADERSIZE, SEEK_SET);
	FREAD(btree->block, blocksize, 1, btree->f);
	int rootlevel = btree->header.maxlevel;
	int levelindex = 0;
	struct btreestack* stack = initbtreestack(btree->block, blocksize, rootlevel);
	bool end = false;
	while ((!end) && (levelindex >= 0) && (ret < offset + limit)) {
		axisbtreeblock block = stack[levelindex].block;
		while ((!end) && (stack[levelindex].i <= block->num))	//= block->num for including last node
		{
			axisbtreenode node = BTREEGETNODE(block, stack[levelindex].i, keysize);
			int r = (stack[levelindex].i == block->num) ? -2 : memcmp(keyfrom, node->key, keysize);
			if (r < 0) {						//including last node
				if (node->child > 0) {
					//optimized
					r = (stack[levelindex].i == block->num) ? -2 : memcmp(keyto, node->key, keysize);
					if ((r >= 0) && (ret > 0) && (node->num != 0) && (ret + node->num + 1 < offset))
					{
						ret += node->num + 1;					
						if (r == 0) end = true;
						else stack[levelindex].i++; 
					}
					else {
						levelindex++;
						FSEEK(btree->f, node->child, SEEK_SET);
						FREAD(stack[levelindex].block, blocksize, 1, btree->f);
						stack[levelindex].i = 0;
						block = stack[levelindex].block;
					}
				}
				else {
					r = (stack[levelindex].i == block->num)? -2:memcmp(keyto, node->key, keysize);
					if (r >= 0) {		//select
						if (ret < offset + limit) {
							if (ret >= offset)  value[ret - offset] = node->data;
							ret++;
						}
						if (r == 0) end = true;
						else stack[levelindex].i++;
					}
					else {
						stack[levelindex].i++;
						if (r == -1) end = true;
					}
				}
			}
			else if (r == 0) {
				//select
				if (ret < offset + limit) {
					if (ret >= offset) value[ret - offset] = node->data;
					ret++;
				}
				else end = true;
				stack[levelindex].i++;
			}
			else
				stack[levelindex].i++;
		}
		levelindex--;
		if (levelindex >= 0) {		//select the parent node too
			axisbtreenode node = BTREEGETNODE(stack[levelindex].block, stack[levelindex].i, keysize);
			if ((stack[levelindex].i != stack[levelindex].block->num) && (memcmp(keyto, node->key, keysize) >= 0)) {		//select
				if (ret < offset + limit) {
					if (ret >= offset)  value[ret - offset] = node->data;
					ret++;
				}
			}
			stack[levelindex].i++;
		}
	}
	destroybtreestack(stack, rootlevel);
	return (ret > offset) ? ret - offset : 0;
}

DATABASETYPE countuniquerangebtree(axisbtree btree, const char* keyfrom, const char* keyto)
{
	int ret = 0;
	int keysize = btree->header.keysize;
	int blocksize = BTREEBLOCKSIZE(btree->header.keysize, btree->header.numkey);
	//get root level
	FSEEK(btree->f, BTREEHEADERSIZE, SEEK_SET);
	FREAD(btree->block, blocksize, 1, btree->f);
	int rootlevel = btree->header.maxlevel;
	int levelindex = 0;
	struct btreestack* stack = initbtreestack(btree->block, blocksize, rootlevel);
	bool end = false;
	while ((!end) && (levelindex >= 0)) {
		axisbtreeblock block = stack[levelindex].block;
		while ((!end) && (stack[levelindex].i <= block->num))	//= block->num for including last node
		{
			axisbtreenode node = BTREEGETNODE(block, stack[levelindex].i, keysize);
			int r = (stack[levelindex].i == block->num) ? -2 : memcmp(keyfrom, node->key, keysize);
			if (r < 0) {						//including last node
				if (node->child > 0) {
					//optimized
					r = (stack[levelindex].i == block->num) ? -2 : memcmp(keyto, node->key, keysize);
					if ((r>= 0) && (node->num != 0) && (ret>0))
					{
						ret += node->num+1;
						if (r == 0) end = true;
						else stack[levelindex].i++;
					} else {
						levelindex++;
						FSEEK(btree->f, node->child, SEEK_SET);
						FREAD(stack[levelindex].block, blocksize, 1, btree->f);
						stack[levelindex].i = 0;
						block = stack[levelindex].block;
					}
				}
				else {
					r = (stack[levelindex].i == block->num) ? -2 : memcmp(keyto, node->key, keysize);
					if (r>=0) {		//select
						ret++;
						if (r == 0) end = true;
						else stack[levelindex].i++;
					}
					else {
						stack[levelindex].i++;
						if (r == -1) end = true;
					}
				}
			}
			else if (r == 0) {
				//select
				ret++;
				stack[levelindex].i++;
			}
			else
				stack[levelindex].i++;
		}
		levelindex--;
		if (levelindex >= 0) {		//select the parent node too
			axisbtreenode node = BTREEGETNODE(stack[levelindex].block, stack[levelindex].i, keysize);
			if ((stack[levelindex].i!=stack[levelindex].block->num) && (memcmp(keyto, node->key, keysize) >= 0)) {		//select
				ret++;
			}
			stack[levelindex].i++;
		}
	}
	destroybtreestack(stack, rootlevel);
	return ret;
}

void lockbtree(axisbtree btree)
{
	BTREEWRITESTATUS(btree, AXISTABLESTATUSWRITE);
}

void unlockbtree(axisbtree btree)
{
	BTREEWRITESTATUS(btree, AXISTABLESTATUSNORMAL);
}

void printbtree(const char* filename,int numblock)
{
	struct axisbtreefileS file;
	FILEPTR f = FOPEN(filename, "rb");
	if (f == NULL) return;
	FREAD(&file, BTREEHEADERSIZE, 1, f);
	printf("type: %d\n", (int)file.type);
	printf("keysize: %d\n", (int)file.keysize);
	printf("numkey: %d\n", (int)file.numkey);
	printf("numblock: %d\n", (int)file.numblock);
	printf("numnode: %d\n", (int)file.numnode);
	printf("maxlevel: %d\n", (int)file.maxlevel);
	int keysize = file.keysize;
	DATABASETYPE blocksize=BTREEBLOCKSIZE(file.keysize, file.numkey);
	axisbtreeblock block = (axisbtreeblock)ALLOCMEM(blocksize);
	DATABASETYPE i = BTREEHEADERSIZE;
	int count = 0;
	while (i < file.numblock * blocksize) {
		FREAD(block, blocksize, 1, f);
		printf("%d %d %d\n", i, block->num, block->parentnode);
		for (int j = 0; j <= block->num; j++)
		{
			axisbtreenode node = BTREEGETNODE(block, j, keysize);
			printf("%s:%d(%d,%d) ", node->key,node->data,node->num,node->child);
		}
		printf("\n");
		i += blocksize;
		count++;
		if (count > numblock) break;
	}
	FREEMEM(block);
	FCLOSE(f);
}