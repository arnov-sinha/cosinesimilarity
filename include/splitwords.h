#ifndef SPLITWORDS_H_INCLUDED
#define SPLITWORDS_H_INCLUDED

#include <vector>
#include <string>

class Splitwords
  {
  private:
  std::vector<std::string> words ;
  size_t nwords ;
  size_t vectorsize ;

  public:

  inline void resize( size_t n )
    {
    if( vectorsize < n )
      {
      words.resize( n );
      vectorsize = words.size() ;
      }

    for( size_t i = nwords ; i < n ; ++i )
      words[ i ].clear() ;

    nwords = n ;
    }

  inline void replace( const std::string &w, size_t pos )
    {
    if( pos >= nwords)
      resize( pos + 1 ) ;

    words[ pos ] = w ;
    }

  inline void swap( std::string &nw, size_t pos )
    {
    if( pos >= nwords )
      resize( pos + 1 ) ;

    words[ pos ].swap( nw ) ;
    }

  inline size_t size( void ) const
    {
    return nwords ;
    }

  inline const std::string& operator[]( size_t n ) const
    {
    return( words[ n ] ) ;
    }

  size_t split( const std::string &s,
                const char delim,
                bool allowempty = true )
    {
    nwords = 0 ;
    size_t len = s.size() ;

    if( len > 0 )
      {
      const char *p = &( s[ 0 ] ) ;
      size_t lo = 0 ;
      size_t hi = 0 ;

      while( lo < len )
        {
        for( ; hi < len ; ++hi )
          if( p[ hi ] == delim )
            break;

        size_t ncopy = hi - lo ;

        if( allowempty || ( ncopy > 0 ) )
          {
          if( nwords >= vectorsize )
            {
            words.resize( 2 * vectorsize ) ;
            vectorsize = words.size() ;
            }
            std::string &word = words[ nwords ] ;
            word.resize( ncopy ) ;

            for( size_t i = 0; i < ncopy; ++i )
              word[ i ] = p[ lo + i ] ;
            ++nwords ;
          }  //  end of if
        lo = ++hi ;
        } //  end of while

      if( allowempty && ( s[ len - 1 ] == delim ) )
        resize( nwords + 1 ) ;
      } //  end of if
    return nwords ;
    }

  Splitwords( size_t initsizearg = 16 ) : nwords( 0 ), vectorsize( 0 )
    {
    const size_t initsize = ( initsizearg == 0 ) ? 1 : initsizearg ; // must be positive
    words.resize( initsize ) ;
    for( size_t i = 0 ; i < initsize ; ++i )
      words[ i ].reserve( 128 ) ;
    vectorsize = words.size() ;
    }

  ~Splitwords()
    {}
  } ;

#endif
