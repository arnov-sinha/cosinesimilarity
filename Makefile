IDIR=include
SDIR=src
ODIR=obj
LDIR=lib

CC=gcc
CXX=g++
CFLAGS=-I$(IDIR)
CXXFLAGS=-I$(IDIR)
COPT= -O2
CXXOPT= -O2
COPTIONS= $(COPT) -g -Wall
TEST=n

ifeq ($(TEST), y)
	CXXOPTIONS= $(CXXOPT) -g -std=c++14 -fopenmp -Wall -D_TEST
else
	CXXOPTIONS= $(CXXOPT) -g -std=c++14 -fopenmp -Wall #-D_DEBUGCORPUS -D_PRINTS
endif

LIBS=

_DEPS = cosinehelper.h splitwords.h quadgramanchors.h
DEPS = $(patsubst %,$(IDIR)/%,$(_DEPS))

_OBJ = cosinehelper.o compute.o
OBJ = $(patsubst %,$(ODIR)/%,$(_OBJ))

$(ODIR)/%.o: $(SDIR)/%.c $(DEPS)
	$(CC) $(COPTIONS) -c -o $@ $< $(CFLAGS)

$(ODIR)/%.o: $(SDIR)/%.cpp $(DEPS)
	$(CXX) $(CXXOPTIONS) -c -o $@ $< $(CXXFLAGS)

cosine: $(OBJ)
	$(CXX) $(CXXOPTIONS) -o $@ $^ $(CXXFLAGS) $(LIBS)

test:
	make -f Makefile TEST=y

.PHONY: clean

clean:
	rm -f gmon.out $(ODIR)/*.o cosine *~ core $(INCDIR)/*~
