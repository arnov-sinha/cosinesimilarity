#ifndef QUTILS_H_INCLUDED
#define QUTILS_H_INCLUDED

#include <sstream>
#include <iomanip>
#include <string>
#include <vector>
#include <unordered_set>
#include <map>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <iconv.h>

class qutils
  {
  private :

  static const std::unordered_set < std::string > countynames ;
  static std::map<std::string,std::string> biznamemappings ;
  static iconv_t iconvutf8desc ;

  public :

  static void increment_stats( const bool incr_n,
                               int64_t &n,
                               const double value,
                               double &mean,
                               double &var );

  static void increment_nstats( const bool incr_n,
                                int64_t &n,
                                const int64_t num,
                                double &mean,
                                double &var );

  static std::string fixcounty( const std::string &str );
  static std::string fixrecordcounty( const std::string &str );
  static std::string fixvehiclecounty( const std::string &str );
  static std::string fixcriminalcounty( const std::string &str );
  static std::string fixpropertycounty( const std::string &str );
  static std::string fixnnumber( const std::string &str );
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

  static std::string makemytimebracketed( void )
    {
    return( std::string("[") + makemytime() + "] " ) ;
    }

  static void parseuri( const std::string &spec, std::string &protocol,
                       std::string &addr, std::string &port, std::string &errstr )
    {
    // IPV4 only!

    protocol.clear() ;
    addr.clear() ;
    port.clear() ;
    errstr.clear() ;

    // Parse things that look like protocol://address:port
    // Example: "tcp://127.0.0.1:3415" has
    //    protocol = "tcp"
    //    address  = "127.0.0.1"
    //    port     = 3415

    uint64_t loc = spec.find( "://" ) ;
    if( loc != spec.npos )
      {
      protocol = spec.substr( 0, loc ) ;
      addr = spec.substr( loc + 3 ) ;
      }
    else
      addr = spec ;

    loc = addr.find_last_of( ':' ) ;
    if( loc != addr.npos )
      {
      port = addr.substr( loc + 1 ) ;
      addr = addr.substr( 0, loc ) ;
      }
    }

  static void uritonumberformat( const std::string &uri, std::string &protocol,
                                 std::string &numericaddr, std::string &numericport,
                                 std::string &errstr )
    {
    static const std::string funcname( "uritonumberformat: " ) ;
    try
      {
      errstr.clear() ;
      numericaddr.clear() ;
      numericport.clear() ;

      struct addrinfo hints ;
      struct addrinfo *addrlist ;

      std::string protocolpart ;
      std::string addresspart ;
      std::string portpart ;

      parseuri( uri, protocolpart, addresspart, portpart, errstr ) ;

      if( errstr.size() > 0  )
        {
        errstr = funcname + "\"" + uri + "\" has improper format" ;
        return ;
        }

      for( uint64_t i = 0 ; i < sizeof( hints ) ; ++i )
        ( ( char * ) &hints )[ i ] = 0 ;

      hints.ai_family = AF_INET ;        // Force IPV4
      hints.ai_socktype = SOCK_STREAM ;  // should consult protocolpart for this

      int result = getaddrinfo( addresspart.c_str(), portpart.c_str(),
                                &hints, &addrlist ) ;

      if( result != 0 )
        {
        errstr = funcname + "getaddrinfo: has failed for \"" +
                 uri + "\" reason: " + gai_strerror( result ) ;
        return ;
        }

      if( addrlist != NULL )
        {
        char ipstr[ NI_MAXHOST ] ;
        char srvstr[ NI_MAXSERV ] ;

        result = getnameinfo( addrlist->ai_addr, sizeof( struct sockaddr_in ),
                              ipstr, sizeof( ipstr ), srvstr, sizeof( srvstr ),
                              NI_NUMERICHOST | NI_NUMERICSERV ) ;

        freeaddrinfo( addrlist ) ;

        if( result != 0 )
          {
          errstr = funcname + "getnameinfo: has failed for \"" +
                   uri + "\" reason: " + gai_strerror( result ) ;
          return ;
          }

        protocol = protocolpart ;
        numericaddr = ipstr ;
        numericport = srvstr ;
        }
      else
        errstr = funcname + "unable to convert \"" + uri + "\" to numeric form" ;
      }

     catch( ... )
       {
       errstr = funcname + "has encountered an unexpected C++ exception" ;
       }
    }

  static std::vector<std::string> &split( const std::string &s,
                                          const char delim,
                                          std::vector<std::string> &elems);
  static std::string getreqid( const std::string &input );
  static std::string joinstringvec( const std::vector<std::string> &input );
  static std::string fixpobox( const std::string &addr ) ;
  static std::string trim( const std::string& str,
                           const std::string& whitespace = " \t");
  static inline bool isspace( char c )
    {
    switch( c )
      {
      case '\t' :
      case '\n' :
      case '\v' :
      case '\f' :
      case '\r' :
      case ' '  :
        return( true ) ;
        break ;

      default :
        return( false ) ;
      }

    return( false ) ;
    }
  static std::string fixutf8( const std::string &str) ;
  static std::string fixstr( const std::string &str);
  static std::string fixalpha( const std::string &str_in);
  static std::string fixname( const std::string &str);
  static std::string fixfirstname( const std::string &str);
  static std::string fixmiddlename( const std::string &str);
  static std::string fixlastname( const std::string &str);
  static std::string fixaddr( const std::string &str);
  static std::string fixdob( const std::string &str );
  static std::string fixssn( const std::string &str );
  static std::string fixzip( const std::string &str );
  static std::string fixbankruptcycasenum( const std::string &str );
  static std::string fixfixedwidthnumber( const std::string &str, uint64_t width );
  static std::string fixphonenumber( const std::string &str );
  static std::string fixbizname( const std::string &str) ;
  static std::string fixduns( const std::string &str );
  static std::string fixtaxid( const std::string &str );
  static std::string fixindcat( const std::string &str );
  static std::string removespace( const std::string &str );
  static std::string uppercase( const std::string &str );
  static std::string fixip( const std::string &str );
  static std::string fixemail( const std::string &str );
  static std::string nexthigherkey( const std::string &thekey,
                                    unsigned char fillval );
  static std::string int_to_str( int i, int width);
  static void computestringhash( const std::string &instr, std::string &hash ) ;
  static std::string createuuidstr( void );
  static std::string createuuidstr( const std::string &s );
  static int str_to_int(const std::string &s, int base = 0);
  static int64_t str_to_int64(const std::string &s, int base = 0);
  static void str_to_valshardloc64( const std::string &s, uint64_t &val, uint64_t &shard, uint64_t &loc );
  static float str_to_float(const std::string &s);
  static double str_to_double(const std::string &s);
  static bool getvmstats( uint64_t &vmsize, uint64_t &vmpeak );
  static std::string showblobloc( const std::string &str ) ;
  static std::string simpleuri( const std::string &uri ) ;
  static void nextmessagesegment( const std::string &msg,
                                  uint64_t startpos,
                                  uint64_t maxlen,
                                  uint64_t &segstart,
                                  uint64_t &seglen ) ;
  static uint64_t stringvecsize( const std::vector< std::string > &strvec );
  static void dblmetaphonelastname( const std::string &name,
                                   std::string &meta1,
                                   std::string &meta2 ) ;
  static void dblmetaphonefirstname( const std::string &name,
                                     std::string &meta1,
                                     std::string &meta2 ) ;
  static void nysiislastname( const std::string &name, std::string &nysiisname ) ;
  static void nysiisfirstname( const std::string &name, std::string &nysiisname ) ;
  static void clearbiznamemapping( void ) ;
  static void addbiznamemapping( const std::string &from, const std::string &to ) ;
  };


#endif /* QUTILS_H_INCLUDED */
