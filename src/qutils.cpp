#include <iostream>
#include <fstream>
#include <sstream>              // ostringstream
#include <iomanip>
#include <cctype>               // toupper
#include <cstdlib> // For string <--> int
#include <climits> // ...
#include <stdexcept> // ...
#include <stdint.h>
#include <errno.h>
#include <locale.h>
#include <uuid/uuid.h>
#include <arpa/inet.h>
#include <omp.h>
#include "qutils.h"

using namespace std;

// must not be smaller than Max[INET_ADDRSTRLEN, INET6_ADDRSTRLEN]
//   static const size_t _ipMaxLen = (INET6_ADDRSTRLEN >= INET_ADDRSTRLEN) ? INET6_ADDRSTRLEN : INET_ADDRSTRLEN ;
static const size_t _ipMaxLen = 45 + 1;

static const uint64_t maxstrlen = 512;

static const uint64_t dblmetaphonelastnamesize = 5 ;
static const uint64_t dblmetaphonefirstnamesize = 4 ;
static const uint64_t nysiislastnamesize = 5 ;
static const uint64_t nysiisfirstnamesize = 4 ;

const unordered_set<string> qutils::countynames = { "CITY",
                                                    "COUNTY",
                                                    "PARISH",
                                                    "PARRISH",
                                                    "BOROUGH", // correct spelling
                                                    "BORROUGH", // misspellings ...
                                                    "BORO",
                                                    "BOROGH",
                                                    "BORROGH",
                                                    "BUROUGH",
                                                    "BUROGH",
                                                    "BURROUGH",
                                                    "BURROGH",
                                                    "BURROW",
                                                    "BURRO", // valid borough abbrev
                                                    "BURRITO", // easter egg 1
                                                    "CENSUS",
                                                    "AREA",
                                                    "MUNICIPALITY",
                                                    "MUNICIPIO",
                                                    "DISTRICT",
                                                    "REGION",
                                                    "CTY", // county abbrev
                                                    "CO",  // county abbrev
                                                    "COU", // county abbrev
                                                    "PAR", // parish abbrev
                                                    "MUN", // municipality / municipio abbrev
                                                    "DIST"}; // district abbrev

map<string,string> qutils::biznamemappings ;

void qutils::increment_stats( const bool incr_n,
                              int64_t &n,
                              const double value,
                              double &mean,
                              double &var )
  {
  if (incr_n)
    {
    ++n;
    }
  double delta = value - mean;
//  if ( n < 1 )
//    {
//    cerr << "INCREMENT_STATS:  n == 0" << endl;
//    printStacktrace();
//    }
  mean += delta / n;
  var  += delta * ( value - mean );
  return;
  }

void qutils::increment_nstats( const bool incr_n,
                               int64_t &n,
                               const int64_t num,
                               double &mean,
                               double &var )
  {
  double value = double(num);
  // int64_t n_init = n;
  return qutils::increment_stats( incr_n, n, value, mean, var );
  // if (incr_n && n_init == n) cerr << "INCREMENT_NSTATS NOT INCREMENTING" << endl;
  }

// See http://stackoverflow.com/questions/236129/split-a-string-in-c
//
// Split a string on a single character delimiter, return a vector of strings
vector<string> &qutils::split( const string &s,
                               const char delim,
                               vector<string> &elems)
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
      elems.push_back( string( &p[lo], hi-lo ) );
      ++hi;
      lo=hi;
      }
    }
  return elems;
}

// Use '|' to split and join request id and search tickers.
// utility function
string qutils::getreqid( const string &input )
  {
  vector<string> elems;
  split(input, '|', elems);
  if (elems.size() == 0) elems.push_back("");
  return elems[0];
  }

string qutils::joinstringvec( const vector< string > &input )
  {
  long n = input.size();
  string result;
  static const string joiner = "|";

  if (n > 0)
    result = input[0];

  for (long i=1; i<n; ++i)
    result += joiner + input[i];

  return result;
  }

string qutils::trim( const string &str,
                     const string &whitespace )
  {
  const size_t strBegin = str.find_first_not_of(whitespace);
  if (strBegin == string::npos)
    return ""; // no content

  const size_t strEnd = str.find_last_not_of(whitespace);
  const size_t strRange = strEnd - strBegin + 1;

  return(str.substr(strBegin, strRange));
  }

string qutils::fixpobox( const string &addr )
  {
  static const vector<string> poboxforms =
    {
    "P O BOX",
    "P. O BOX",
    "P O. BOX",
    "P.O BOX",
    "PO. BOX",
    "P.O. BOX",
    "P. O. BOX",

    "POBOX",
    "P OBOX",
    "P. OBOX",
    "P O.BOX",
    "PO. BOX",
    "P.O.BOX",

    "PO BX",
    "P O BX",
    "P. O BX",
    "P O. BX",
    "P.O BX",
    "PO. BX",
    "P.O. BX",
    "P. O. BX",

    "PO BX.",
    "P O BX.",
    "P. O BX.",
    "P O. BX.",
    "P.O BX.",
    "PO. BX.",
    "P.O. BX.",
    "P. O. BX.",

    "POBX",
    "P OBX",
    "P. OBX",
    "P O.BX",
    "P.O.BX",

    "POB",
    "P.OB",
    "PO.B",
    "POB.",
    "P.O.B",
    "P.OB.",
    "PO.B.",
    "P.O.B.",

    "POST BOX",

    "POST OFFICE BOX"
    } ;
  static const string pobox( "PO BOX" ) ;

  string poboxfixed = addr ;
  uint64_t i ;
  uint64_t npoboxforms ;
  uint64_t addrlen ;
  uint64_t len ;
  uint64_t loc ;
  bool changed ;

  npoboxforms = poboxforms.size() ;

  do
    {
    changed = false ;
    addrlen = poboxfixed.size() ;

    for( i = 0 ; i < npoboxforms ; ++i )
      {
      loc = poboxfixed.find( poboxforms[ i ] ) ;
      len = poboxforms[ i ].size() ;                // Must be present
      if( ( loc != string::npos ) &&                // Must be beginning of line or following a space
          ( loc > 0 ) && ( poboxfixed[ loc - 1 ] != ' ' ) )
        loc = string::npos ;
      if( ( loc != string::npos ) &&                // Must end at end of line or preceding a space
          ( ( loc + len ) < addrlen ) && ( poboxfixed[ loc + len ] != ' ' ) )
        loc = string::npos ;

      if( loc != string::npos )
        {
        poboxfixed = poboxfixed.substr( 0, loc ) +
                     pobox +
                     poboxfixed.substr( loc + len ) ;
        changed = true ;
        break ;
        }
      }
    }
  while( changed ) ;

  return( poboxfixed ) ;
  }

string qutils::fixutf8( const string &str )
  {
  static vector<iconv_t> iconvutf8desc ;
  static bool init = false ;

  string out ;

  if( !init )
    {
#pragma omp critical( initutf8 )
      {
      if( !init )
        {
        const uint64_t iconvsize = 512 ;
        setlocale( LC_ALL, "en_US.utf8" ) ; // This can fail!
        iconvutf8desc.resize( iconvsize ) ;
        for( uint64_t i = 0 ; i < iconvsize ; ++i )
          iconvutf8desc[ i ] = iconv_open( "ASCII//TRANSLIT", "UTF-8" ) ;  // This can fail!
        }
      init = true ;
#pragma omp flush ( init )
      } // end of critical
    }

  uint64_t threadnum = omp_get_thread_num() ;
  //  Threadcount exceeding iconvutf8desc size will not be processed
  if( ( threadnum < iconvutf8desc.size() ) && ( iconvutf8desc[ threadnum ] != ( iconv_t ) -1 ) )
    {
    iconv( iconvutf8desc[ threadnum ], NULL, NULL, NULL, NULL ) ;            // Reset to initial state

    size_t inbytesleft = str.size() ;
    out.resize( 2 * inbytesleft ) ;
    size_t outbytesleft = out.size() ;
    char *p = ( char * ) str.data() ;
    char *q = ( char * ) out.data() ;

    size_t result = iconv( iconvutf8desc[ threadnum ], &p, &inbytesleft,
                                          &q, &outbytesleft ) ;
    if( result == ( size_t ) -1 )                               // Failed.  Give up trying to convert
      out = str ;
    else
      out.resize( out.size() - outbytesleft ) ;
    }
  else
    out = str ;

  return( out ) ;
  }

string qutils::fixstr(const string &str)
  {
  // pack multiple spaces and remove leading and trailing spaces, uppercase remainder
  uint64_t n ;
  string out ;
  string cleanstr( fixutf8( str ) ) ;

  n = cleanstr.size() ;
  if( n > 0 )
    {
    uint64_t i ;
    uint64_t j ;
    char *p ;
    char *q ;

    out.resize( n ) ;
    p = ( char * ) cleanstr.data() ;
    q = ( char * ) out.data() ;

    j = 0 ;
    i = 0 ;
    for( ; i < n ; ++i )          // Eliminate leading spaces
      if( !isspace( p[ i ] ) )
        break ;

    while( i < n )
      {
      for( ; i < n ; ++i )
        {
        if( isspace( p[ i ] ) )
          break ;
        q[ j++ ] = toupper( p[ i ] ) ;
        }

      for( ; i < n ; ++i )          // Skip spaces
        if( !isspace( p[ i ] ) )
          break ;

      if( i < n )                   // Keep one space
        q[ j++ ] = ' ' ;
      }

    out.resize( j ) ;
    }

  return( out ) ;
  }

string qutils::removespace(const string &str)
  {

  // Remove all spaces from string
  uint64_t n = str.size();
  string out;
  if ( n > 0 )
    {
    out.resize(maxstrlen);
    uint64_t pack = 0;
    for (uint64_t iter=0; iter < n; ++iter)
      {
      if (! isspace(str[iter]))
        {
        out[pack++] = toupper(str[iter]); // upcase string char
        if (pack == maxstrlen)
          break;
        }
      }
    out.resize(pack);           // finish compression
    }
  return out;
  }

string qutils::uppercase(const string &str)
  {
  // Uppercase string, no other changes
  uint64_t n = str.size();
  string out;
  if ( n > 0 )
    {
    out.resize(n);
    for (uint64_t iter=0; iter < n; ++iter)
      out[iter] = toupper(str[iter]); // upcase string char
    }
  return out;
  }

static int IPADDRCompliance(char *ip, char IPOut[_ipMaxLen])
  {
  struct addrinfo hint, *res = NULL;
  struct sockaddr_in *sockaddr_ip;
  struct sockaddr_in6 *sockaddr_ipv6;
  int ret;

  for(uint64_t i = 0; i < sizeof(hint); ++i)
    ((char*)&(hint))[i] = '\0';

  hint.ai_family = PF_UNSPEC;
  hint.ai_flags = AI_NUMERICHOST;

  //preprocess the ip address to remove all white spaces
  char* cpy = ip;
  char* temp = ip;

  while(*cpy)
    {
    if( !isspace(*cpy) )
      *temp++ = toupper(*cpy);
    cpy++;
    }
  *temp = 0;

  //In the case of IPV4 addresses, getaddrinfo will treat address component beginning
  //with a '0' char as an octal value, and components beginnning with a '0x' as hex values.
  //Important note: This also auto converts any IPV4 style IPV6
  //eg: "::F:4" is represented as "::0.15.0.4"

  ret = getaddrinfo(ip, NULL, &hint, &res); // getaddrinfo parses the IP information

  if( (ret != 0) || ( (res->ai_family != AF_INET) && (res->ai_family != AF_INET6) ) )
    ret = -1;

  else if(res->ai_family == AF_INET) //IP address looks like IPv4
    {
    sockaddr_ip = (struct sockaddr_in *) res->ai_addr;
    if(inet_ntop(AF_INET,&(sockaddr_ip->sin_addr), IPOut, _ipMaxLen) != NULL) //Converts into the IPv4 standard format
      ret = 4;
    else
      ret = -1 ;
    }

  else if (res->ai_family == AF_INET6) //IP address looks like IPv6
    {
    sockaddr_ipv6 = (struct sockaddr_in6 *) res->ai_addr;
    if(inet_ntop(AF_INET6,&(sockaddr_ipv6->sin6_addr), IPOut, _ipMaxLen) != NULL) //Converts into the IPv6 standard format
      ret = 6;
    else
      ret = -1 ;
    }

  freeaddrinfo(res);

  if(ret == 6) //Converting everything to uppercase
    for(uint64_t i = 0; i < _ipMaxLen; ++i )
      IPOut[i]= toupper(IPOut[i]);

  return(ret);
  }//IPADDRCompliance

string qutils::fixip(const string &str)
  {
    char outIP[_ipMaxLen];
    string inIP(str);
    int ipType = (inIP.size() > 0) ? IPADDRCompliance(&(inIP[0]), &(outIP[0])) : -1;

    if(ipType != -1)
      return(outIP);

    return(removespace(fixutf8(str)));
  }

string qutils::fixemail(const string &str)
  {
  return(removespace(fixutf8(str)));
  }

string qutils::fixalpha(const string &str_in)
  {
  // keep only single spaces, remove non-alpha, and uppercase
  string str(fixstr(str_in));
  uint64_t n = str.size();
  string out;
  if ( n > 0 )
    {
    out.resize(maxstrlen);
    uint64_t pack = 0;
    for (uint64_t iter=0; iter < n; ++iter)
      {
      if ((str[iter] == ' ') || isalpha(str[iter]))
        {
        out[pack++] = toupper(str[iter]); // upcase string char
        if (pack == maxstrlen)
          break;
        }
      }
    out.resize(pack);           // finish compression
    }
  return out;
  }

string qutils::fixname( const string &str )
  {
  static const vector<string> elimtrailing =
    {
    "1", " 1", "  1", "1ST", " 1ST", "  1ST",
    "2", " 2", "  2", " II", "  II", "11", " 11", "  11", "2ND", " 2ND", "  2ND",
    "3", " 3", "  3", " III", "  III", "111", " 111", "  111", "3RD", " 3RD", "  3RD",
    "4", " 4", "  4", " IV", "  IV", "4TH", " 4TH", "  4TH", " IIII", "  IIII",
    "5", " 5", "  5", "5TH", " 5TH", "  5TH",
    "6", " 6", "  6", "6TH", " 6TH", "  6TH",
    "7", " 7", "  7", "7TH", " 7TH", "  7TH",
    "8", " 8", "  8", "8TH", " 8TH", "  8TH",
    "9", " 9", "  9", "9TH", " 9TH", "  9TH",
    "SR", " SR", "  SR",
    "JR", " JR", "  JR"
    " DDS", "PHD", " PHD", "DMD", " DMD", " MD", "PHD DR", " PHD DR", "DR PHD", " DR PHD"
    } ;

  // keep only alpha numeric after eliminating special ending terms
  string cleanutf8 = fixutf8( str ) ;
  string out ;
  out.reserve( maxstrlen ) ;
  uint64_t n = cleanutf8.size() ;
  uint64_t i ;


  // Remove leading and trailing spaces and
  //        leading and trailing non-alphanumeric chars
  // Remove non-alphanumeric chars but keep spaces
  // Collapse internal multiple contiguous spaces to a single space
  // Uppercase remaining characters
  for( i = 0 ; i < n ; ++i )                // Remove trailing non-alphanumeric
    if( isalnum( cleanutf8[ n - i - 1 ] ) )
      break ;

  n -= i ;

  for( i = 0 ; i < n ; ++i )                // Remove leading non-alphanumeric
    if( isalnum( cleanutf8[ i ] ) )
      break ;

  while( i < n )
    {
    if( isalnum( cleanutf8[ i ] ) )         // Keep Numbers and Uppercase letters
      {
      out.push_back( toupper( cleanutf8[ i ] ) ) ;
      ++i ;
      }
    else if( isspace( cleanutf8[ i ] ) )     // Keep one space
      {
      out.push_back( ' ' ) ;                 //   (make sure it is space and not tab, etc.)
      ++i ;

      while( ( i < n ) && !isalnum( cleanutf8[ i ] ) ) // Skip until next alphanumeric
        ++i ;
      }
    else                                     // Skip non-space/alphanumeric
      ++i ;
    }

  n = out.size() ;

  static const string nassr( "NASSR" ) ;
  static const string nasr( "NASR" ) ;
  bool specialend = ( ( n >= nassr.size() ) && ( out.substr( n - nassr.size() ) == nassr ) ) ||
                    ( ( n >= nasr.size()  ) && ( out.substr( n - nasr.size()  ) == nasr ) ) ;

  uint64_t nelim = elimtrailing.size() ;
  uint64_t longestmatch = 0 ;
  for( i = 0 ; i < nelim ; ++i )             // Find longest matching suffix to be removed
    {
    uint64_t elimlen = elimtrailing[ i ].size() ;
    if( ( n >= elimlen ) && ( elimlen > longestmatch ) &&
        ( out.substr( n - elimlen, n ) == elimtrailing[ i ] ) &&
        ( !specialend || ( elimtrailing[ i ] != "SR" ) ) )
        longestmatch = elimlen ;
    }

  n -= longestmatch ;                        // Remove longest matching suffix

  uint64_t j = 0 ;                           // Remove anything left that is not alpha-numeric (e.g. spaces)
  for( i = 0 ; i < n ; ++i )
    if( isalnum( out[ i ] ) )
      {
      out[ j ] = out[ i ] ;
      ++j ;
      }

  n = j ;

  if( n > maxstrlen )                        // Limit searchterm length
    n = maxstrlen ;

  out.resize( n ) ;

  return( out ) ;
  }

string qutils::fixfirstname(const string &str)
  {
  return(fixname(str));
  }

string qutils::fixmiddlename(const string &str)
  {
  return(fixname(str));
  }

string qutils::fixlastname(const string &str)
  {
  return(fixname(str));
  }

string qutils::fixcounty(const string &str)
  {
  static const string ste("STE") ;
  static const string st("ST") ;
  vector<string> words ;
  // Split input string into words separated by spaces (after removing)
  split(fixalpha(str),' ',words);

  string result;

  uint64_t n = words.size() ;

  for (uint64_t i=0; i<n; ++i)
    {
    if ( words[i].find("SAINTE") == 0 )
      words[i] = ste + words[i].substr(6) ;
    else if ( words[i].find("SAINT") == 0 )
      words[i] = st + words[i].substr(5) ;
    }

  if (n == 0)
    return(result);
  else if (n == 1)
    return(words[0]);

  // loop over last words or word pairs until all county designators removed
  while (n > 1)
    {
    if (countynames.count(words[n-1]) == 0)
      break;
    --n;
    }

  // concatenate remaining words
  for (uint64_t i=0; i<n; ++i)
    result += words[i];

  return(result);
  }

string qutils::fixbizname(const string &str)
  {
  static const string empty ;
  static const string space( " " ) ;

  string cleanutf8 = fixutf8( str ) ;
  string workstr ;
  uint64_t nstr = cleanutf8.size() ;
  workstr.reserve( nstr ) ;

  for( uint64_t i = 0 ; i < nstr ; ++i )
    {
    char ch = cleanutf8[ i ] ;          // Discard everything except:
    if( ch == '&' )                     //   Convert & to space
      workstr.push_back( ' ' ) ;
    else if( isspace( ch ) )            //   Preserve spaces
      workstr.push_back( ' ' ) ;
    else if( isalpha( ch ) )            //   Keep letters as upper case
      workstr.push_back( toupper( ch ) ) ;
    else if( isdigit( ch ) )            //   Keep digits
      workstr.push_back( ch ) ;
    else if( ( ch == '+' ) &&           // TODO EXPERIMENTAL: keep '+' if it is the first
             ( workstr.size() == 0 ) )  // interesting character to signal a bizname range search
      workstr.push_back( ch ) ;         
    else if( ( ch == '~' ) )            // TODO EXPERIMENTAL: keep '~' if it is the first
                                        // interesting character to signal business cosinesimiliarity
      workstr.push_back( ch ) ;
    }

  vector<string> words ;                // Now transform selected words into canonical form
  split( workstr, ' ', words ) ;

  uint64_t nwords = words.size() ;
                                        // Now collapse adjacent single character words
  for( uint64_t i = 0 ; i < nwords ;  )
    {
    if( words[ i ].size() == 1 )
      {
      uint64_t j  = 0 ;
      for( j = i + 1 ; j < nwords ; ++j )
        if( words[ j ].size() == 1 )
          {
          words[ i ] += words[ j ] ;
          words[ j ].clear() ;
          }
        else if( words[ j ].size() > 1 )
          break ;

      i = j ;
      }
    else
      ++i ;
    }
  
  for( uint64_t i = 0 ; i < nwords ; ++i )  // Eliminate trailing 'S' for words with at least 3 chars
    if( ( words[ i ].size() >= 3 ) &&
        ( words[ i ][ words[ i ].size() - 1 ] == 'S' ) )
      words[ i ].resize( words[ i ].size() - 1 ) ;

  for( uint64_t i = 0 ; i < nwords ; ++i )   // Map words in bizname to correct for goofy abbrvs, etc.
    if( words[ i ].size() > 0 )
      {
      map<string,string>::const_iterator it = biznamemappings.find( words[ i ] ) ;
      if( it != biznamemappings.end() )
        words[ i ] = it->second ;
      }

  uint64_t lastwordind = UINT64_MAX ;            // Find right-most non-blank word
  for( uint64_t i = 0 ; i < nwords ; ++i )
    if( words[ nwords - i - 1 ].size() > 0 )
      {
      lastwordind = nwords - i - 1 ;
      break ;
      }

  if( lastwordind < nwords )                     // Now some special processing
    {                                            // Trim deleteable suffixes off of last word if it has a minimum size
    uint64_t wordlen = words[ lastwordind ].size() ;
    if( wordlen >= 6 )
      {                                          //   PLLC *BEFORE* LLC!
      static const vector<string> deletesuffixes = { "PLLC", "LLC", "INC", "LTD", "COM" } ;

      bool action = false ;
      uint64_t ndelete = deletesuffixes.size() ;
      for( uint64_t id = 0 ; id < ndelete ; ++id )
        {
        uint64_t deletelen = deletesuffixes[ id ].size() ;
        if( words[ lastwordind ].substr( wordlen - deletelen ) == deletesuffixes[ id ] )
          {
          words[ lastwordind ] = words[ lastwordind ].substr( 0, wordlen - deletelen ) ;
          action = true ;
          break ;
          }
        }

      if( !action )
        {                                        // Split separable suffixes of the lsat word into two words
        static const vector<string> separatesuffixes = { "MD", "CPA", "ORG" } ;

        uint64_t nseparate = separatesuffixes.size() ;
        for( uint64_t is = 0 ; is < nseparate ; ++is )
          {
          uint64_t separatelen = separatesuffixes[ is ].size() ;
          if( words[ lastwordind ].substr( wordlen - separatelen ) == separatesuffixes[ is ] )
            {
            words[ lastwordind ] = words[ lastwordind ].substr( 0, wordlen - separatelen ) ;
            words.push_back( separatesuffixes[ is ] ) ;
            ++nwords ;
            action = true ;
            break ;
            }
          }
        }
      }
    }

  workstr.clear() ;                     // Assemble fixed name
  for( uint64_t i = 0 ; i < nwords ; ++i )
    if( words[ i ].size() > 0 )
      workstr += ( ( workstr.size() > 0 ) ? space : empty ) + words[ i ] ;

  if( workstr.size() > maxstrlen )      // Limit max length
    workstr.resize( maxstrlen ) ;
  
  return( workstr ) ;
  }

string qutils::fixduns( const string &str )
  {
  const uint64_t dunswidth = 9 ;

  return( fixfixedwidthnumber( str, dunswidth ) ) ;
  }

string qutils::fixtaxid( const string &str )
  {
  const uint64_t taxidwidth = 9 ;

  return( fixfixedwidthnumber( str, taxidwidth ) ) ;
  }

string qutils::fixindcat( const string &str )
  {
  const uint64_t indcatwidth = 6 ;

  return( fixfixedwidthnumber( str, indcatwidth ) ) ;
  }

string qutils::fixrecordcounty(const string &str)
  {
  return(fixcounty(str));
  }

string qutils::fixvehiclecounty(const string &str)
  {
  return(fixcounty(str));
  }

string qutils::fixcriminalcounty(const string &str)
  {
  return(fixcounty(str));
  }

string qutils::fixpropertycounty(const string &str)
  {
  return(fixcounty(str));
  }

string qutils::fixaddr(const string &str)
  {
  // pack multiple spaces and remove leading and trailing spaces
  // Turn commas to space:
  uint64_t n = str.size();
  string out = fixutf8( str ) ;
  uint64_t minlen = (n<maxstrlen) ? n : maxstrlen ;
  out.resize(minlen) ;
  for (uint64_t i=0; i < minlen; ++i)
    out[i] = (out[i] != ',') ? out[i] : ' ';
  return(fixpobox(fixstr(out)));
  }

string qutils::fixdob( const string &str )
  {
  string out = qutils::fixstr( str );

  if ( out.size() == 10U )      // Reorder to YYYYMMDD if DOB has structure
    {                           // MM.DD.YYYY or YYYY.MM.DD, where separator '.' = '/' or '-'
    if ( isdigit( out[ 0 ] ) && isdigit( out[ 1 ] )   && // MM
         ( ( out[ 2 ] == '/') || ( out[ 2 ] == '-') ) && // separator
         isdigit( out[ 3 ] ) && isdigit( out[ 4 ] )   && // DD
         ( ( out[ 5 ] == '/') || ( out[ 5 ] == '-') ) && // separator
         isdigit( out[ 6 ] ) && isdigit( out[ 7 ] ) && isdigit( out[ 8 ] ) && isdigit( out[ 9 ] ) // YYYY
       )
      return out.substr( 6, 4 ) + out.substr( 0, 2 ) + out.substr( 3, 2 ) ;

    else if ( isdigit( out[ 0 ] ) && isdigit( out[ 1 ] ) && isdigit( out[ 2 ] ) && isdigit( out[ 3 ] ) && // YYYY
              ( ( out[ 4 ] == '/') || ( out[ 4 ] == '-') ) && // separator
              isdigit( out[ 5 ] ) && isdigit( out[ 6 ] )   && // MM
              ( ( out[ 7 ] == '/') || ( out[ 7 ] == '-') ) && // separator
              isdigit( out[ 8 ] ) && isdigit( out[ 9 ] )      // DD
            )
      return out.substr( 0, 4 ) + out.substr( 5, 2 ) + out.substr( 8, 2 ) ;
    }

  if( out.size() > maxstrlen )
    out.resize( maxstrlen ) ;

  return(out);
  }

string qutils::fixssn( const string &str )
  {
  string fixed = qutils::fixstr( str );
  uint64_t fixed_size = fixed.size();

  string out;

  static const string single_zero( "0" ) ;
  static const string double_zero( "00" ) ;

  // collapse hyphens
  out.resize(9);
  uint64_t out_size = 0;
  for (uint64_t i = 0; i<fixed_size; ++i)
    {
    if ( fixed[i] != '-' )
      {
      out[out_size] = fixed[i];
      ++out_size;
      if ( out_size == out.size() )
        break;
      }
    }

  out.resize(out_size);

  // Further length adjustments:
  if ( out_size == 8)
    out = single_zero + out;
  else if ( out_size == 7 )
    out = double_zero + out;

  // Truncate to last 4 or first 5 if leading or trailing zeros:
  if ( out.size() == 9 )
    {
    bool first5zero = (out.compare(0,5,"00000") == 0) ;
    bool last4zero  = (out.compare(5,4,"0000") == 0) ;

    if ( first5zero && (! last4zero) )
      out = out.substr(5,4) ;
    else if (last4zero)
      out.resize(5) ;
    }

  // 9 zeros case is trapped in qhloadsearchterms.m4.cpp

  return(out);
  }

string qutils::fixzip( const string &str )
  {
  string fixed = qutils::fixstr( str );
  int64_t fixed_size = fixed.size();

  string out;

  static const string single_zero( "0" ) ;
  static const string double_zero( "00" ) ;

  // collapse hyphens
  out.resize(fixed_size);

  int64_t out_size = 0;
  for (int64_t i = 0; i<fixed_size; ++i)
    {
    if ( fixed[i] != '-' )
      {
      out[out_size] = fixed[i];
      ++out_size;
      }
    }
  out.resize(out_size);

  string result;
  if ( out_size == 8)
    result = single_zero + out;
  else if ( out_size == 7 )
    result = double_zero + out;
  else if ( out_size == 4)
    result = single_zero + out;
  else if ( out_size == 3 )
    result = double_zero + out;
  else
    result = out;

  if ( result.size() > 5)
    result.resize(5);

  return(result);
  }

string qutils::fixbankruptcycasenum( const string &str )
  {
  string out = qutils::fixstr(str);

  if( out.size() > maxstrlen )
    out.resize(maxstrlen) ;

  return(out) ;
  }

string qutils::fixfixedwidthnumber( const string &str, uint64_t width )
  {
  string cleanutf8 = fixutf8( str ) ;
  uint64_t n_str = cleanutf8.size();
  string out;
  out.resize(width);
  uint64_t out_size = 0;
  for (uint64_t i=0; i<n_str; ++i)
    {
    if (isdigit(cleanutf8[i]))
      {
      out[out_size]=cleanutf8[i];
      ++out_size;
      if ( out_size == width )
        break;
      }
    }
  out.resize(out_size) ;
  return(out);
  }

string qutils::fixphonenumber( const string &str )
  {
  const uint64_t phonewidth = 10 ;

  return( fixfixedwidthnumber( str, phonewidth ) ) ;
  }

string qutils::nexthigherkey( const string &thekey, unsigned char fillval )
  {
  string nxtkey ;
  bool exists ;
  long len ;
  unsigned char *uthekey ;
  unsigned char *unxtkey ;

  len = thekey.size() ;

  uthekey = ( unsigned char * ) &( thekey[ 0 ] ) ;

  exists = false ;
  for( long i = 0 ; i < len ; ++i )
    if( uthekey[ i ] != 255 )
      {
      exists = true ;
      break ;
      }

  if( exists )
    {
    nxtkey.resize( len ) ;
    unxtkey = ( unsigned char * ) &( nxtkey[ 0 ] ) ;

    unsigned long carry = 1 ;
    for( long i = len - 1 ; i >= 0 ; --i )
      {
      if( ( ( unsigned long ) uthekey[ i ] + carry ) > 255UL )
        {
        unxtkey[ i ] = 0 ;
        carry = 1 ;
        }
      else
        {
        unxtkey[ i ] = uthekey[ i ] + carry ;
        carry = 0 ;
        }
      }
    }
  else
    {
    nxtkey.resize( len + 1 ) ;
    unxtkey = ( unsigned char * ) &( nxtkey[ 0 ] ) ;
    for( long i = 0 ; i < len ; ++i )
      unxtkey[ i ] = uthekey[ i ] ;
    unxtkey[ len ] = fillval ;
    }

  return( nxtkey ) ;
  }

string qutils::int_to_str(int i, int width)
  {
  ostringstream oss;
  oss.fill('0');
  oss.width(width);
  oss << i;
  return oss.str();
  }

// Handle ints up to 2G
int qutils::str_to_int(const string &s, int base)
  {
  char *end;
  errno = 0;
  long result = strtol(s.c_str(), &end, base);
  if (errno == ERANGE || result > INT_MAX || result < INT_MIN)
    throw out_of_range("str_to_int: string is out of range");
  if (s.length() == 0 || *end != '\0')
    throw invalid_argument("str_to_int: invalid string");
  return result;
  }

int64_t qutils::str_to_int64(const string &s, int base)
  {
  char *end;
  errno = 0;
  int64_t result = strtoll(s.c_str(), &end, base);
  if (errno == ERANGE )
    throw out_of_range("str_to_int64: string is out of range");
  if (s.length() == 0 || *end != '\0')
    throw invalid_argument("str_to_int64: invalid string");
  return result;
  }

void qutils::str_to_valshardloc64(const string &s,
                                  uint64_t &val,
                                  uint64_t &shard,
                                  uint64_t &loc)
  {
  for (int64_t q=0; q<8; ++q)
    ((unsigned char *) &(val))[q] = *((unsigned char *) &(s[q]));
  shard = ( val >> 56 ) & 0xff ;
  loc = val & 0x00ffffffffffffffL ;
  }

float qutils::str_to_float(const string &s)
  {
  char *end;
  errno = 0;
  float result = strtof(s.c_str(), &end);
  if (errno == ERANGE)
    throw out_of_range("str_to_float: string is out of range");
  if (s.length() == 0 || *end != '\0')
    throw invalid_argument("str_to_float: invalid string");
  return result;
  }

double qutils::str_to_double(const string &s)
  {
  char *end;
  errno = 0;
  double result = strtod(s.c_str(), &end);
  if (errno == ERANGE)
    throw out_of_range("str_to_double: string is out of range");
  if (s.length() == 0 || *end != '\0')
    throw invalid_argument("str_to_double: invalid string");
  return result;
  }

bool qutils::getvmstats( uint64_t &vmsize, uint64_t &vmpeak )
  {
  vmsize = 0;
  vmpeak = 0;
  fstream f;
  f.open("/proc/self/status", ios::in);
  if (f.is_open())
    {
    string s;
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
        string units = s.substr(s.size()-2);
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

string qutils::showblobloc( const string &str )
  {
  stringstream interpretation ;
  streamsize w = interpretation.width() ;
  interpretation << setfill(' ') ;
  if( str.size() == sizeof( uint64_t ) )
    {
    uint64_t val ;
    uint64_t shard ;
    uint64_t loc ;

    for( uint64_t j = 0 ; j < sizeof( uint64_t ) ; ++j )
      ( ( char * ) &val )[ j ] = str[ j ] ;
    shard = ( val >> 56 ) & 0xff ;
    loc = val & 0x00ffffffffffffffL ;
    interpretation << "["
                   << right << setw(3) << shard
                   << setw(w) << ":"
                   << left << setw(20) << loc
                   << setw(w) << "]" ;
    }
  else
    interpretation << "Unexpected blob loc size " << str.size() ;

  return( interpretation.str() ) ;
  }

string qutils::simpleuri( const string &uri )
  {
  return( (uri.find("tcp://") == 0) ? uri.substr(6) : uri );
  }

void qutils::nextmessagesegment( const string &msg,
                                 uint64_t startpos,
                                 uint64_t maxlen,
                                 uint64_t &segstart,
                                 uint64_t &seglen )
  {
  // find next message 'segment' beginning at startpos
  // If msg[startpos] is a new-line, find next non-new-line char
  // Segment ends just before a new-line, or when segment has maxlen chars, or end of the message
  // segment starting position return in segstart with length seglen
  // seglen = 0 means no more segments
  // startpos should be 0 for beginning of string

  segstart = 0 ;
  seglen = 0 ;

  uint64_t msgsize = msg.size() ;
  for( ; startpos < msgsize ; ++startpos )  // skip newlines
    if( msg[ startpos ] != '\n' )
      break ;

  if( startpos < msgsize )
    {
    uint64_t slen = 0 ;
    for( uint64_t i = startpos ; i < msgsize ; ++i )
      {
      if( ( slen == maxlen ) || ( msg[ i ] == '\n' ) )
        break ;

      ++slen ;
      }

    segstart = startpos ;
    seglen = slen ;
    }
  }

string qutils::fixnnumber(const string &str)
  {
  // Create a valid N-number: "An N-number begins with a run of one or
  //  more numeric digits, may end with one or two alphabetic letters,
  //  may only consist of one to five characters in total, and must
  //  start with a digit other than zero. In addition, N-numbers may
  //  not contain the letters I or O, due to their similarities with
  //  the numerals 1 and 0."
  string cleanutf8 = fixutf8( str ) ;
  uint64_t n = cleanutf8.size();
  string out ;

  if ( n > 0 )
    {
    out.resize(n+1);            // In case leading N is not entered
    uint64_t iter;
    uint64_t pack = 0;
    char c;

    // Iterate past leading spaces/hyphens until a valid
    // country code is found.  If none found, leading
    // char is set to 'N'.
    // NOTE for future:  Codes of more than one letter
    // are currently a problem with this approach, so handle
    // only Canada and US (both one letter) for the present.
    for (iter=0; iter<n; ++iter)
      {
      c = toupper(cleanutf8[iter]);
      switch(c)
        {
        case 'N':
        case 'C':    // Only handle US and Canada at present
          out[pack] = c;
          ++pack;
          goto exit_country_code_loop;
          break;
        case '-':
          break;
        default:
          if (!isspace(c))
            goto exit_country_code_loop;
          break;
        }
      }
    exit_country_code_loop: ;   // goto target for exiting loop

    // Set default country code to US code ('N') if none yet set
    if (pack == 0)
      {
      out[pack] = 'N';
      ++pack;
      }

    // iterate past optional chars, hyphens, spaces,
    // until any other character is found
    for(; iter < n ; ++iter)
      {
      c = toupper(cleanutf8[iter]);
      switch(c)
        {
        case 'N' :
        case 'C' :
        case 'G' :
        case 'L' :
        case 'R' :
        case 'S' :
        case 'X' :
        case '-' :
          // Only iterate past allowed leading characters
          break;
        default:
          if (!isspace(c))                // handle other space characters also
            goto exit_optional_char_loop ; // exit loop without iterating
          break;
        }
      }
    exit_optional_char_loop: ;   // goto target to exit loop

    // Pack digits, ignoring leading zeros, or spaces or hyphens
    // Note that this halts if any other non-digit is found
    for ( ; iter < n ; ++iter)
      {
      c = cleanutf8[iter];

      if (isdigit(c))
        {
        if ((pack > 1) || (c != '0')) // Ignore leading zeros
          {
          out[pack] = c;
          ++pack;
          }
        }
      else if ((!isspace(c)) && (c != '-') )
        break;
      }

    // pack > 1 means we have at least one digit.
    // If we halted the leading char loop with a non-digit,
    // we have safely avoided packing it.
    if (pack == 1)
      {
      pack = 0;                 // Set pack to zero, no digits were found
      }
    else                        // pack > 1
      {
      // Also include any trailing alpha that isn't 'I' or 'O':
      for ( ; iter < n ; ++iter)
        {
        c = toupper(cleanutf8[iter]);

        if (isalpha(c) && (c != 'I') && (c != 'O'))
          {
          out[pack] = c;
          ++pack;
          }
        else if ((!isspace(c)) && (c != '-') )
          {
          pack = 0;             // We found an invalid character, null out the pack
          break;
          }
        }
      }

    if( pack > maxstrlen )
      {
      pack = maxstrlen ;
      }

    out.resize(pack);           // finish compression
    }
  return(out);
  }

uint64_t qutils::stringvecsize( const vector<string> &strvec )
  {
  uint64_t n = strvec.size();
  uint64_t sz = 0;
  for (uint64_t i=0; i<n; ++i)
    sz += strvec[i].size();
  return(sz);
  }

void qutils::clearbiznamemapping( void )
  {
  biznamemappings.clear() ;
  }

void qutils::addbiznamemapping( const string &from, const string &to )
  {
  biznamemappings.insert( pair<string,string>( from, to ) ) ;
  }
