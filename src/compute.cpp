#include <iostream>
#include <stdint.h>
#include <string>
#include <vector>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "cosinehelper.h"

using namespace std ;

int main( int argc, char **argv )
  {
  std::string input ;
  std::vector<std::string> inputcorpus ;
  std::vector<std::vector<Result_t> > result ;
  struct timespec timetoload ;
  static CosineHelper *cos ;

#ifdef _DEBUGCORPUS
  std::vector<std::string> debugcorpus = {  "jean",
                                            "john",
                                            "jean jean",
                                            "john john john",
                                            "ordan",
                                            "jean fred",
                                            "campbell dantecampbell",
                                            "campbell"
                                            // "jonathan",
                                            // "jonathen", 
                                            // "jonah",
                                            // "johnny",
                                            // "joan",
                                            // "jonathans"
                                         } ;
  clock_gettime( CLOCK_REALTIME, &timetoload ) ;
  cos = new CosineHelper( debugcorpus, defaultcleaningtool ) ;
#else
  if( argc == 1 )
    {
    std::cout<<"Usage: "<<argv[ 0 ]
             <<"\n[ -f <filename> ] to be loaded "
             <<"\n[ -n <value grater than 0> ] manual corpus to be loaded "
             <<std::endl ;
    return 1 ;
    }

  if( strcmp( argv[ 1 ], "-f" ) == 0 )
    {
    if( argc > 2 )
      {
      clock_gettime( CLOCK_REALTIME, &timetoload ) ;
      cos = new CosineHelper( argv[ 2 ], defaultcleaningtool ) ;
      }
    else
      {
      std::cout<<"No file provided"<<std::endl ;
      return 1 ;
      }
    }

  if( strcmp( argv[ 1 ], "-n" ) == 0 )
    {
    if( argc > 2 )
      {
      uint64_t n = strtoul( argv[ 2 ],NULL,10 ) ;

      if( n <= 0 )
        {
        std::cout<<"Enter value greater than 0"<<std::endl ;
        return 1 ;
        }
      for( uint64_t i = 0 ;  i < n ; ++i )
        {
      	getline( std::cin, input ) ;
      	inputcorpus.push_back( input ) ;
      	}
      clock_gettime( CLOCK_REALTIME, &timetoload ) ;
      cos = new CosineHelper( inputcorpus, defaultcleaningtool ) ;
      }
    else
      {
      std::cout<<"No argument provided"<<std::endl ;
      return 1 ;
      }
    }
#endif

  std::cout<<"Time to load the corpus: "<<compute_elapsed( timetoload )<<std::endl ;

#ifdef _DEBUGCORPUS
  std::cout<<"Current corpus-> "<<std::endl ;
  for( uint64_t i = 0 ; i < debugcorpus.size() ; ++i )
    std::cout<<"\t\t\t"<<debugcorpus[ i ]<<std::endl ;
  std::cout<<std::endl ;
#endif

  cos->stats() ;

#ifdef _TEST
  input.clear() ;
  input = "Andew" ; 
  result = cos->cosinematching( input ) ;
  std::cout<<"\nCosine results ->"<<std::endl ;
  for( uint64_t i = 0 ; i < result[ 0 ].size() ; ++i )
    std::cout<<"[ "<<i+1<<" ] "<<result[ 0 ][ i ].part<<"\t "<<result[ 0 ][ i ].score<<"\t\t\t"<<std::endl ;
  return 0 ;

#endif  
  input.clear() ;
  while( true )
    {
    std::cout<<"\nEnter the input string for testing Cosine Similarity (quit to exit): "<<std::endl ;
    getline(std::cin,input) ;
    if( input.compare("quit") )
      result = cos->cosinematching( input ) ;
    else
      return 0 ;
    
    std::cout<<"\nCosine results ->"<<std::endl ;
    for( uint64_t i = 0 ; i < result[ 0 ].size() ; ++i )
      std::cout<<"[ "<<i+1<<" ] "<<result[ 0 ][ i ].part<<"\t "<<result[ 0 ][ i ].score<<"\t\t\t"<<std::endl ;
                                 // <<result[ 1 ][ i ].part<<"\t "<<result[ 1 ][ i ].score<<"\t\t\t"<<std::endl ;
                                 // <<result[ 2 ][ i ].part<<"\t "<<result[ 2 ][ i ].score<<std::endl ; 
    }
  }
