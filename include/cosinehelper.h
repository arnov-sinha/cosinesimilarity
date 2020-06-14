#ifndef COSINEHELPER_H_INCLUDED
#define COSINEHELPER_H_INCLUDED

#include <iostream>
#include <iomanip>
#include <stdint.h>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <algorithm>
#include <sstream>
#include <fstream>
#include <sys/time.h>
#include <unordered_set>
#include <unordered_map>
#include <omp.h>
#include "segmentedvector.h"
#include "splitwords.h"
#include "quadgramanchors.h"

typedef struct Result_t
	{
	std::string part ;
	double score ;
	}
	Result_t ;

typedef struct Wordform_t
  {
  char* wordtext ;
  uint32_t* rownnzs ;  // Rownnzs[ 0 ] -> the number of nonzero in this matrix row 
  }
  Wordform_t ;

typedef struct Corpusform_t
  {
  float rowmaginv ;
  uint32_t rowinfoindex ;  // Rowinfoindex -> Index number for row information in corpusrowinfo
  }
  Corpusform_t ;

typedef Segmentedvector<Corpusform_t, 1024ULL * 1024ULL> SV_corpusform ;
typedef Segmentedvector<uint32_t, 1024ULL * 1024ULL> SV_corpusrowinfo ;

class CosineHelper
{
  const char* filename ;
  float inputrowmaginv ;
  uint32_t nbigramcols ;
  uint32_t ntrigramcols ;
  uint32_t nwordcols ;
  uint32_t nmatrixcols ;
  uint64_t totalnnzs ;
	struct timespec timetoload ;
  QuadgramAnchors anchorwords ;
  SV_corpusrowinfo corpusrowinfo ;
  SV_corpusform corpus ;
  std::unordered_map<std::string, uint32_t> wordstolist ;
  std::vector<char*> corpusdata ;
  std::vector<Wordform_t> wordlist ;
	std::vector<double> idf ;
	std::vector<float> rowcofs ;
	std::vector<uint32_t> bigramstodim ;
  std::vector<uint32_t> trigramstodim ;
	std::vector<uint32_t> quadgrams ;
  std::vector<uint32_t> quadgramcount ;
  std::vector<uint8_t> anchormask ;   // When applying anchorwords


private:
	// Loading Corpusdata
  void loadcorpus( const char* filename ) ;
  void loadcorpus( const std::vector<std::string> &inputcorpus ) ;
  bool readblockreadfile( FILE *f,
                   uint64_t preferredbufsize,
                       std::string &prereaddata,
                       std::string &buffer ) ;
  void extractpreread( std::string &preread, std::string &buffer ) ;
  void processblock( uint64_t &nread, uint64_t &nlines,
                       const std::string &buffer ) ;
  void computestarts( const std::string &buf,
                        std::vector<uint64_t> &bufstarts ) ;
  FILE *openblockreadfile( const std::string &filename ) ;
  void closeblockreadfile( FILE *f ) ;
  void prepcorpus( const std::string &buf,
                   std::vector<uint64_t> &bufstarts ) ;
  
  // Dimension words, and form data structures
  void dimensionwords() ;
  void collectwords( ) ;
  void formcorpus() ;
  
  // Form matrix rows, compute magnitudes and compute IDF
  void formmatrix( void ) ;
  void formmatrixrow( const std::string &inputtext, std::vector<uint32_t> &sparserow ) ;
  void formmatrixrow( const std::string &inputtext,
                      std::vector<uint32_t> &sparserow,
                      Splitwords &words,
                      std::vector<uint32_t> &bigrams ,
                      std::vector<uint32_t> &trigrams ,
                      std::vector<uint32_t> &bigramcount ,
                      std::vector<uint32_t> &trigramcount ,
                      std::vector<std::string> &uniqueword,
                      std::vector<uint32_t> &wordcount ) ;
  void computeidf( void ) ;
  void idfDedup( const uint32_t* rownnzs,
                 std::vector<bool> &termsusedthisrow,
                 std::vector<uint32_t> &theseterms
                 ) ;
  void computemagnitude( void ) ;

  void uniquewords( const std::string &data,
                    char delim,
                    Splitwords &splitwords,
                    std::vector<std::string> &uniqueword,
                    std::vector<uint32_t> &wordcount ) ;
  void getuniquetrigrams( const std::string &data,
                    std::vector<uint32_t> &trigrams,
                          std::vector<uint32_t> &trigramcount ) ;
  void getuniquebigrams( const std::string &data,
                           std::vector<uint32_t> &bigrams,
                           std::vector<uint32_t> &bigramcount ) ;

  // Building anchorwords
  void buildanchorwords( void ) ;
  void generatequadgrams(  const std::string &data, 
                            std::set<uint32_t> &myquads ) ;

  // Cosine similarity:
  std::vector<Result_t> score( const std::string inputtext, const std::vector<uint32_t> &inputnnzs,
                               uint64_t maxresults, double threshold,
                               const std::vector<uint32_t> selectedrows, bool tanimoto = false ) ;
  double dotrow( const uint32_t* rowentries ) const ;
  void addtotopscores( uint64_t newindex, 
                         double newscore,
                         std::vector<uint64_t> &rowindexes,
                         std::vector<double> &rowscores ) const ;
  void scatterweights( const std::vector<uint32_t> &rowentries, bool dozero ) ;
  void scatteranchormasks( const std::string &inputtext, uint8_t value ) ;

  // Utilities
  std::string getcorpustext( uint32_t index ) ;
  double f1score( Result_t cosine, Result_t tanimoto ) ;
  std::vector<Result_t> accumscores( std::vector< std::vector<Result_t> > &result, uint64_t maxresults ) ;
  std::string getquadgram( uint32_t anchorgram ) ;
	std::vector<uint32_t> selectrows() ;
	std::string getcorpusmatrixform( uint32_t rowinfoindex ) ;
	std::string ( *cleaningtool ) ( const std::string &dirtystring ) ;
	
	inline uint32_t mapbigramtodim( uint32_t index ) const
	  {
	  const uint32_t bigramoffset = 0 ;

	  return( index < bigramstodim.size() ? bigramstodim[ index ] + bigramoffset : 0UL + bigramoffset ) ;
	  }

	inline uint32_t maptrigramtodim( uint32_t index ) const
	  {
	  uint32_t trigramoffset = nbigramcols ;

	  return( index < trigramstodim.size() ? trigramstodim[ index ] + trigramoffset : 0UL + trigramoffset ) ;
	  }

	inline uint32_t mapwordtodim( const std::string &word ) const
	  {
	  uint32_t wordoffset = nbigramcols + ntrigramcols ;

	  std::unordered_map<std::string,uint32_t>::const_iterator it = wordstolist.find( word ) ;
	  return( ( it != wordstolist.end() ) ? it->second + wordoffset : 0UL + wordoffset ) ;
	  }

	inline static uint32_t entrycreate( uint32_t index, uint32_t weight )
	  {
	  if( weight > 0xff )
	    weight = 0xff ;

	  if( index > 0x00ffffff )
	    index = 0x00ffffff ;

	  return( ( weight << 24 ) | index ) ;
	  }

	inline static uint32_t entryindex( uint32_t entry )
	  {
	  return( entry & 0x00ffffff ) ;
	  }

	inline static uint32_t entryweight( uint32_t entry )
	  {
	  return( ( entry >> 24 ) & 0xff ) ;
	  }

public:
	CosineHelper() ;
	CosineHelper( const char* file, 
				  std::string ( *cleaner ) ( const std::string& ) ) ;
	CosineHelper( const std::vector<std::string> &inputcorpus,
                  std::string ( *cleaner ) ( const std::string& ) ) ;
	~CosineHelper() ;
	std::vector<std::vector<Result_t> > cosinematching( const std::string &input, uint64_t maxresults = 200, double threshold = 0 ) ;
	void stats( void ) ;
} ;

std::string stdcleaningtool( const std::string &dirtystring ) ;
std::string defaultcleaningtool( const std::string &dirtystring ) ;

static inline double compute_elapsed( const struct timespec &starttime)
  {
  struct timespec endtime;
  clock_gettime( CLOCK_REALTIME, &endtime );
  double elapsed = (( endtime.tv_sec +
                      endtime.tv_nsec / ( double ) 1000000000 ) -
                    ( starttime.tv_sec +
                      starttime.tv_nsec / ( double ) 1000000000 )) ;
  return elapsed;
  }

static std::string makemytime( void )
  {
  struct timeval tv ;
  struct tm brokendowntime ;
  std::stringstream ss ;
  long partialsec ;

  gettimeofday( &tv, NULL ) ;
  gmtime_r( &tv.tv_sec, &brokendowntime ) ;
  partialsec = 1000 * tv.tv_usec * 0.000001 ;

  ss << std::setfill( '0' ) << std::setw( 4 ) << brokendowntime.tm_year + 1900 << "-" <<
        std::setfill( '0' ) << std::setw( 2 ) << brokendowntime.tm_mon + 1     << "-" <<
        std::setfill( '0' ) << std::setw( 2 ) << brokendowntime.tm_mday        << "T" <<
        std::setfill( '0' ) << std::setw( 2 ) << brokendowntime.tm_hour        << ":" <<
        std::setfill( '0' ) << std::setw( 2 ) << brokendowntime.tm_min         << ":" <<
        std::setfill( '0' ) << std::setw( 2 ) << brokendowntime.tm_sec         << "." <<
        std::setfill( '0' ) << std::setw( 3 ) << partialsec                    << "Z" ;

  return( ss.str() ) ;
  }

static inline std::string makemytimebracketed()
  {
  return( std::string("[") + makemytime() + "] " ) ;
  }

static int64_t str_to_int64(const std::string &s, int base)
  {
  char *end;
  errno = 0;
  int64_t result = strtoll(s.c_str(), &end, base);
  if (errno == ERANGE )
    throw std::out_of_range("str_to_int64: string is out of range");
  if (s.length() == 0 || *end != '\0')
    throw std::invalid_argument("str_to_int64: invalid string");
  return result;
  }

static inline bool getvmstats( uint64_t &vmsize, uint64_t &vmpeak )
  {
  vmsize = 0;
  vmpeak = 0;
  std::fstream f;
  f.open("/proc/self/status", std::ios::in);
  if (f.is_open())
    {
    std::string s;
    getline(f,s);
    while (f.good())
      {
      int dowhich = 0;
      if (s.find("VmPeak:") == 0)
        {
        dowhich = 1;
        s = s.substr(7);
        }
      else if (s.find("VmSize:") == 0)
        {
        dowhich = 2;
        s = s.substr(7);
        }
      if (dowhich != 0)
        {
        std::string units = s.substr(s.size()-2);
        s = s.substr(0,s.size()-3);
        uint64_t val = 0;
        try
          {
          val = str_to_int64(s,0);
          }
        catch(...)
          {
          vmsize = 0;
          vmpeak = 0;
          return(false);        // BAD ... str_to_int64 failed
          }
        if ( units == "kB" )
          val *= 1024ULL;
        else if ( units == "mB" )
          val *= 1024ULL * 1024ULL ;
        else if ( units == "gB" )
          val *= 1024ULL * 1024ULL * 1024ULL ;
        else                    // BAD, can't parse the units
          {
          vmsize = 0;
          vmpeak = 0;
          return(false);
          }
        if ( dowhich == 1 )
          vmpeak = val;
        else if ( dowhich == 2 )
          vmsize = val;
        else
          {
          vmsize = 0;
          vmpeak = 0;
          return(false);        // BAD, no dowhich
          }
        }
      getline(f,s);
      }
    f.close();
    }
  else
    return(false);              // bad ... failed
  return(true);
  }

static inline std::vector<std::string> &split( const std::string &s,
                                     					 const char delim,
                                     					 std::vector<std::string> &elems)
  {
  int64_t lo;
  int64_t hi;
  int64_t len;
  const char *p;
  elems.clear();
  len = s.size() ;
  if (len > 0)
    {
    p = &(s[0]);

    lo = 0;
    hi = 0;
    while (lo < len)
      {
      for ( ; hi < len ; ++hi )
        if ( p[hi] == delim ) break;
      elems.push_back( std::string( &p[lo], hi-lo ) );
      ++hi;
      lo=hi;
      }
    }
  return elems;
}

#endif
