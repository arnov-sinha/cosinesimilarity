#include <iostream>
#include <vector>
#include <algorithm>
#include <stdint.h>
#include <mutex>
#include <atomic>
#include <memory>

class QuadgramAnchors
  {
  private :

  static const uint32_t ntopslots = 65536 ;
  std::atomic<bool> toplocks[ ntopslots ] ;
  std::vector<std::vector<uint32_t> > quadsused ;            // sorted, 1-1 with quadtorows
  std::vector<std::vector<std::vector<uint32_t> > > quadtorows ;  // indicates rows involving a particular quad
  std::unordered_set<uint32_t> discard ;
  const uint32_t thresh ;


  private:

  void associaterow( uint8_t c1, uint8_t c2, uint8_t c3, uint8_t c4, uint32_t rownum )  // convert this to chars
    {
    uint32_t slot = getslots( c1, c2 ,c3 ,c4 ) ;

    while( std::atomic_exchange_explicit( &toplocks[ slot ], true, std::memory_order_acquire ) ) ;  // Spin lock till acquired

    uint32_t quadcode = genquadcode( c1, c2, c3, c4 ) ;

    // Find quad in the used quads (like a binary search)
    std::vector<uint32_t>::iterator it = lower_bound( quadsused[ slot ].begin(),
                                                      quadsused[ slot ].end(), quadcode ) ;

    if( ( it == quadsused[ slot ].end() ) || ( *it != quadcode ) ) // First time a particular quad is seen
      {
      static std::vector<uint32_t> empty ;
      uint32_t index = 0 ;
      if( ( it - quadsused[ slot ].begin() ) > 0 )
        index = it - quadsused[ slot ].begin() ;

      std::vector<std::vector<uint32_t> >::iterator qit = quadtorows[ slot ].begin() + index ;
      quadsused[ slot ].insert( it, quadcode ) ;
      quadtorows[ slot ].insert( qit, empty ) ;
      quadtorows[ slot ][ index ].push_back( rownum ) ;
      }
    else
      {
      uint64_t index = it - quadsused[ slot ].begin() ;
      uint32_t currsize = quadtorows[ slot ][ index ].size() ;
      if( currsize < thresh )
        quadtorows[ slot ][ index ].push_back( rownum ) ;
      else
        {
        discard.insert( genquadcode( c1, c2, c3, c4 ) ) ;
        std::vector<uint32_t>().swap( quadtorows[ slot ][ index ] ) ;
        }
      }
    std::atomic_store_explicit( &toplocks[ slot ], false, std::memory_order_release ) ;
  }

  public :

  QuadgramAnchors( const uint32_t cutoff = UINT32_MAX ) : thresh( cutoff )
    {
    for( uint32_t i = 0 ; i < ntopslots ; ++i )
      toplocks[ i ] = false ;

    quadsused.resize( ntopslots ) ;
    quadtorows.resize( ntopslots ) ;
    
    if( thresh != UINT32_MAX )
      discard.reserve( 22000 ) ;
    }

  ~QuadgramAnchors()
    {
    }

  uint32_t size( void ) const
    {
    uint32_t sum = 0;
    for( uint32_t i = 0 ; i < ntopslots ; ++i )
      sum += quadsused[ i ].size() ;
    return sum ;
    }

  std::vector<uint32_t> getusedvecs( void ) const 
    {
    std::vector<uint32_t> usedvecs ;
    uint32_t quadsusedsize = quadsused.size() ;
    for( uint32_t i = 0 ; i < quadsusedsize ; ++i )
      {
      uint32_t nquads = quadsused[ i ].size() ;
      uint32_t usedvecssize = usedvecs.size() ;
      usedvecs.resize( usedvecssize + nquads ) ;
      for( uint32_t j = 0 ; j < nquads ; ++j )
        usedvecs[ usedvecssize + j ] = quadsused[ i ][ j ] ;
      }
    return usedvecs ;
    }

  static inline uint32_t getslots( uint8_t c1 , uint8_t c2, uint8_t c3, uint8_t c4 )
    {
    return ( c1 * 256UL + c2 ) ;
    }

  static inline uint32_t genquadcode( uint8_t c1, uint8_t c2, uint8_t c3, uint8_t c4 )
    {
    return ( ( uint32_t ( c1 ) << 24 ) | ( uint32_t ( c2 ) << 16 ) | ( uint32_t ( c3 ) << 8 ) | uint32_t ( c4 ) ) ;
    }

  static inline void inversequadcode( uint32_t code, uint8_t &c1, uint8_t &c2, uint8_t &c3, uint8_t &c4 )
    {
    c1 = ( code >> 24 ) & 0xff ;
    c2 = ( code >> 16 ) & 0xff ;
    c3 = ( code >> 8 ) & 0xff;
    c4 = code & 0xff;
    }

  static inline void inversequadcodereadable( uint32_t code )
    {
    uint8_t c1 = ( code >> 24 ) & 0xff ;
    uint8_t c2 = ( code >> 16 ) & 0xff ;
    uint8_t c3 = ( code >> 8 ) & 0xff;
    uint8_t c4 = code & 0xff;
    std::cout<<c1<<c2<<c3<<c4 ;
    }

  inline uint32_t getdiscardcount( void )
    {
    return discard.size() ;
    }

  void compactor( void )
    {
    uint64_t total = 0 ;
    const uint64_t worthit = 10000 ;

#pragma omp parallel
    {
    for( uint32_t i = 0 ; i < ntopslots ; ++i )
      {
      uint32_t level2size = quadtorows[ i ].size() ;

#pragma omp for schedule( static ) nowait
      for( uint32_t j = 0 ; j < level2size ; ++j )
        { 
        uint32_t capacity = quadtorows[ i ][ j ].capacity() ;
        uint32_t size = quadtorows[ i ][ j ].size() ;
        
        if( ( capacity - size ) > worthit )
          {
#pragma omp atomic update
          total += quadtorows[ i ][ j ].capacity() ;
          
          {                                                             // temp gets scopes out
          std::vector<uint32_t> temp( quadtorows[ i ][ j ] ) ;
          quadtorows[ i ][ j ].swap( temp ) ;
#pragma omp atomic update
          total -= quadtorows[ i ][ j ].capacity() ;
          }

          }
        }
      }
    }// end of parallel

    std::cout<<"Total capacity saved -> "<<total<<std::endl ;
    }

  void associaterow( uint32_t code, uint32_t rownum, bool force )
    {
    uint8_t c1 ;
    uint8_t c2 ;
    uint8_t c3 ;
    uint8_t c4 ;
    std::unordered_set<uint32_t>::const_iterator it = discard.find( code ) ;
    if( force || it == discard.end() )
      {
      inversequadcode( code, c1, c2, c3, c4 ) ;
      // inversequadcodereadable( code ) ;
      // std::cout<<std::endl <<std::endl ;
      associaterow( c1, c2, c3, c4, rownum ) ;
      }
    }

  const std::vector<uint32_t> &getquadrows( uint8_t c1, uint8_t c2, uint8_t c3, uint8_t c4 )
    {
    uint32_t slot = getslots( c1, c2, c3, c4 ) ;
    static const std::vector<uint32_t> empty ;

    uint32_t quadcode = genquadcode( c1, c2, c3, c4 ) ;
    // Find quad in the used quads (like a binary search)
    std::vector<uint32_t>::iterator it = lower_bound( quadsused[ slot ].begin(),
                                                      quadsused[ slot ].end(), quadcode ) ;

    return( ( ( it == quadsused[ slot ].end() ) || ( *it != quadcode ) ) ? empty
                : quadtorows[ slot ][ it - quadsused[ slot ].begin() ] ) ;
    }

  const std::vector<uint32_t> &getquadrows( uint32_t code )
    {
    uint8_t c1 ;
    uint8_t c2 ;
    uint8_t c3 ;
    uint8_t c4 ;
    inversequadcode( code, c1, c2, c3, c4 ) ;
    return getquadrows( c1, c2, c3, c4 ) ;
    }

  void stats( void )
    {
    // should use locks here
    std::cout<<"\nTotal size of Quads -> "<<size()<<std::endl<<std::endl ;
    for( uint32_t i = 0 ; i < ntopslots ; ++i )
      { 
      uint32_t quadsize = quadsused[ i ].size() ;
      for( uint32_t j = 0 ; j < quadsize ; ++j )
        { 
        std::cout<<"Quadgrams \"" ;
        inversequadcodereadable(quadsused[ i ][ j ]) ;
        std::cout<<"\""<<" nrows: "<<quadtorows[ i ][ j ].size()<<std::endl ;
        // std::cout<<"QuadIndexes -> \t\t" ; 
        // for( uint32_t k = 0 ; k < quadtorows[ i ][ j ].size() ; ++k )
        //   std::cout<<quadtorows[ i ][ j ][ k ]<<" " ;
        // std::cout<<std::endl ;
        }
      }
    }
  } ;
