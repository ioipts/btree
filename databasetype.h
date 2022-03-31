/*
 * Copyright (c) 2022 Pit Suwongs, พิทย์ สุวงศ์ (Thailand)
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote
 * products derived from this software without specific prior written
 * permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * By Pit Suwongs <admin@ornpit.com>
 */
 
#ifndef DATABASE_TYPE_H_
#define DATABASE_TYPE_H_

/**
* @file databasetype.h
* @brief Database Type
* Machine specification
*/

#if defined(_MSC_VER)
#include <windows.h>
#pragma warning( disable : 4996 )				// disable deprecated warning
#if !defined(_CRT_SECURE_NO_WARNINGS)
#define _CRT_SECURE_NO_WARNINGS
#endif
#endif
#include <stdio.h>
#include <stdlib.h>	
#include <stdint.h>
#include <string.h>

#define PACKED 
#define FORCE_INLINE

#if defined(_M_X64) 
#define FILEPTR FILE*
#define FOPEN fopen
#define FWRITE fwrite
#define FREAD fread
#define FSEEK _fseeki64
#define FTELL _ftelli64
#define FFLUSH fflush
#define FCLOSE fclose
#else
#define FILEPTR FILE*
#define FOPEN fopen
#define FWRITE fwrite
#define FREAD fread
#define FSEEK fseek
#define FTELL ftell
#define FFLUSH fflush
#define FCLOSE fclose
#endif

#define ALLOCMEM(S) malloc((S))
#define REALLOCMEM(X,S) realloc((X),(S))
#define CALLOCMEM(S,N) calloc((S),(N))
#define FREEMEM(X) free((X))
#define SETMEM(X,V,S) memset((X),(V),(S))
#define CPYMEM(X,O,S) memcpy((X),(O),(S))

#if defined(_32BIT_FILE) || !(defined(__x86_64__)  || defined(_M_X64)  || defined(__LP64__) || defined(__APPLE__))
#define DATABASETYPE uint32_t
#else
#define DATABASETYPE uint64_t
#endif

#define AXISTABLESTRSIZE 128

/**
* All constant
*/
#define AXISTABLEVERSIONBIT32 1
#define AXISTABLEVERSIONBIT64 2
#define AXISTABLEVERSIONBITMASK 3

#define AXISTABLESTATUSNORMAL 0
#define AXISTABLESTATUSWRITE 1


#define NULLDATA 0
/**
* 4 bytes or 8 bytes depend on system
*/
#define IDDATA 1
#define BYTEDATA 2
#define WORDDATA 3
#define INTDATA 4
#define LONGDATA 5
#define FLOATDATA 6
#define DOUBLEDATA 7
#define DATETIMEDATA 8
#define STRINGDATA 9

extern unsigned short FIELDSIZE[];

#define axisfieldsize(T,S) (FIELDSIZE[(T)]==0)?(S):FIELDSIZE[(T)];

#define EQUALDATA 0
#define MOREDATA 1
#define LESSDATA 2
#define BETWEENDATA 3

#pragma pack(push,1)

typedef struct axisfieldS* axisfield;
struct axisfieldS {
	unsigned short id;		//begin at 1
	unsigned short type;
	unsigned short size;
	char name[AXISTABLESTRSIZE];
} PACKED;

typedef struct axisfieldsS* axisfields;
struct axisfieldsS {
	DATABASETYPE blocksize;
	unsigned short num;
	axisfield fields;
} PACKED;

typedef char* ptr;

typedef struct axiscellS* axiscell;
struct axiscellS {
	axisfields fields;
	DATABASETYPE numrows;
	/**
	* pointer to data ex. *(int*)cell for int
	*/
	ptr cell;
} PACKED;

#endif