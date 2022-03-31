EXECUTABLE=testbtree
CC=g++
OBJ_PATH=.
ifndef OS
OS=$(shell uname)
OSMINOR=$(shell uname -m)
endif

ifeq ($(OS),Darwin)		
  LOCAL_PATH=..
  PROJ_PATH=.
  SEP=/
  CN=rm -rf 
  #support for Mac M1 x86_64
  #CFLAGS=-c -Wall -Wno-address-of-packed-member -F/Library/Frameworks -target x86_64-apple-macos11.00
  #LDFLAGS=-F/Library/Frameworks -target x86_64-apple-macos11.00 
  CFLAGS=-c -Wall -Wno-address-of-packed-member -F/Library/Frameworks
  LDFLAGS=-F/Library/Frameworks -lm -ldl  
else ifeq ($(OS),Linux)
  LOCAL_PATH=..
  PROJ_PATH=.
  LDFLAGS=-lm -ldl  
  CFLAGS=-c -Wall -Wno-unused-result -O3 
  SEP=/
  CN=rm -rf
else ifeq ($(OS),Windows_NT)
  LOCAL_PATH=..
  PROJ_PATH=.
  SEP=\\
  CN=del /q
  CFLAGS=-c -Wall -O3
  LDFLAGS=-lm 
endif

INCFLAGS=-I$(LOCAL_PATH) -I$(PROJ_PATH) 

SOURCES=$(PROJ_PATH)$(SEP)sample.cpp 
SOURCES+=$(LOCAL_PATH)$(SEP)axisbtree.cpp

OBJECTS=$(SOURCES:.cpp=.o)
OBJECTS+=$(CSOURCES:.c=.o)

all: $(SOURCES) $(EXECUTABLE)
	
$(EXECUTABLE): $(OBJECTS) 
	$(CC) $(addprefix $(OBJ_PATH)$(SEP),$(notdir $(OBJECTS))) -o $@ $(LDFLAGS)

.cpp.o:
	$(CC) $(INCFLAGS) $(CFLAGS) $< -o $(addprefix $(OBJ_PATH)$(SEP),$(notdir $@))
.c.o:
	$(CC) $(INCFLAGS) $(CFLAGS) $< -o $(addprefix $(OBJ_PATH)$(SEP),$(notdir $@))
	
clean:
	$(CN) $(OBJ_PATH)$(SEP)*.o
