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
CXXOPTIONS= $(CXXOPT) -g -std=c++14 -fopenmp -Wall #-D_DEBUGCORPUS -D_PRINTS

LIBS=

_DEPS = cosinehelper.h splitwords.h quadgramanchors.h
DEPS = $(patsubst %,$(IDIR)/%,$(_DEPS))

_OBJ = cosinehelper.o compute.o
OBJ = $(patsubst %,$(ODIR)/%,$(_OBJ))

$(ODIR)/%.o: $(SDIR)/%.c $(DEPS)
	$(CC) $(COPTIONS) -c -o $@ $< $(CFLAGS)

$(ODIR)/%.o: $(SDIR)/%.cpp $(DEPS)
	$(CXX) $(CXXOPTIONS) -c -o $@ $< $(CXXFLAGS)

runcosine: $(OBJ)
	$(CXX) $(CXXOPTIONS) -o $@ $^ $(CXXFLAGS) $(LIBS)

.PHONY: clean

clean:
	rm -f gmon.out $(ODIR)/*.o runcosine *~ core $(INCDIR)/*~
