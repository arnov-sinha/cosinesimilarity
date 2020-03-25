#ifndef SEGMENTEDVECTOR_H_INCLUDED
#define SEGMENTEDVECTOR_H_INCLUDED

#include <vector>
#include <initializer_list>

// Implments a mini-container that is a "segmented vector".
// The intention is that it act mostly like and STL std::vector
// but not require all elements to be contiguous - it stores
// the data as a vector of array segments.  This also means
// that adjacent elements are not necessarily contiguous in
// memory.
//
// An example use is where one needs a long vector, but
// can get into trouble if it must be resized larger.
// std::vector might create a new vector and copy into it and
// then delete the old data.  This causes memory bloat and
// incurs copy time.  This implementation just adds more
// segments when that situation occurs.
//
// In another example, a large vector object will be required
// to be contigous in memory which can cause virtual memory
// fragmentation or can imply a much larger than necessary
// virtual memory size to manage the large vectors.  This
// implementation can represent the perhaps very large vectors
// as a collection of presumably much smaller sub-vectors.

template<class T, size_t SEGSIZE >
class Segmentedvector
  {
  private :

  size_t cursize ;
  std::vector<std::vector<T> > segments ;

  public :

  Segmentedvector() : cursize( 0 )
    {
    }

  explicit Segmentedvector( size_t n ) : cursize( n )
    {
    size_t nsegs = n / SEGSIZE ;
    if( ( nsegs * SEGSIZE ) < n )
      ++nsegs ;

    segments.resize( nsegs ) ;
    for( size_t i = 0 ; i < nsegs ; ++i )
      segments[ i ].resize( SEGSIZE ) ;
    }

  Segmentedvector( size_t n, const T &val ) : cursize( n )
    {
    size_t nsegs = n / SEGSIZE ;
    if( ( nsegs * SEGSIZE ) < n )
      ++nsegs ;

    segments.resize( nsegs ) ;
    for( size_t i = 0 ; i < nsegs ; ++i )
      segments[ i ].resize( SEGSIZE, val ) ;
    }

  Segmentedvector( const Segmentedvector &other ) : cursize( other.cursize ),
                                                    segments( other.segments )
    {
    }

  ~Segmentedvector()
    {
    }

  Segmentedvector & operator=( const Segmentedvector &rhs )
    {
    if( this != &rhs )
      {
      cursize = rhs.cursize ;
      segments = rhs.segments ;
      }

    return( *this ) ;
    }

  inline T &operator[] ( size_t n )
    {
    return( segments[ ( n / SEGSIZE ) ][ n - SEGSIZE * ( n / SEGSIZE ) ] ) ;
    }

  inline const T &operator[] ( size_t n ) const
    {
    return( segments[ ( n / SEGSIZE ) ][ n - SEGSIZE * ( n / SEGSIZE ) ] ) ;
    }

  void swap( Segmentedvector &other )
    {
    size_t sizetmp ;

    sizetmp = other.cursize ;
    other.cursize = cursize ;
    cursize = sizetmp ;
    segments.swap( other.segments ) ;
    }

  inline size_t size( void ) const
    {
    return( cursize ) ;
    }

  inline size_t capacity( void ) const
    {
    return( SEGSIZE * segments.size() ) ;
    }

  void reserve( size_t n )
    {
    size_t nsegs = n / SEGSIZE ;
    if( ( nsegs * SEGSIZE ) < n )
      ++nsegs ;

    size_t curnsegs = segments.size() ;
    if( nsegs > curnsegs )
      {
      segments.resize( nsegs ) ;
      for( size_t i = curnsegs ; i < nsegs ; ++i )
        segments[ i ].resize( SEGSIZE ) ;
      }
    }

  void resize( size_t n )
    {
    if( n > cursize )
      {
      size_t nsegs = n / SEGSIZE ;
      if( ( nsegs * SEGSIZE ) < n )
        ++nsegs ;

      size_t curnsegs = segments.size() ;
      if( nsegs > curnsegs )
        {
        segments.resize( nsegs ) ;
        for( size_t i = curnsegs ; i < nsegs ; ++i )
          segments[ i ].resize( SEGSIZE ) ;
        }
      }

    cursize = n ;
    }

  void shrink_to_fit( void )
    {
    size_t nsegs = cursize / SEGSIZE ;
    if( ( nsegs * SEGSIZE ) < cursize )
      ++nsegs ;

    if( nsegs > segments.size() )
      segments.shrink_to_fit() ;
    }

  void clear( void )
    {
    cursize = 0 ;
    std::vector<std::vector<T> >().swap( segments ) ;
    }
  } ;

#endif