#include <string.h>
#include <math.h>
#include "cosinehelper.h"

using namespace std ;

CosineHelper::CosineHelper( const char* file, 
                            string ( *cleaner ) ( const string& dirtystring ) ) : filename( file ),
                                                                                  totalnnzs( 0 ),
                                                                                  anchorwords( 15000 ),
                                                                                  cleaningtool( *cleaner )
  {
  loadcorpus( filename ) ;
  }

CosineHelper::CosineHelper( const std::vector<std::string> &inputcorpus,
                            string ( *cleaner ) ( const string& dirtystring ) ) : totalnnzs( 0 ), cleaningtool( *cleaner )
  {
  loadcorpus( inputcorpus ) ;
  dimensionwords() ;
  formmatrix() ;
  buildanchorwords() ;
  }

CosineHelper::~CosineHelper()
  {
  uint32_t wordlistsize = wordlist.size() ;

  for( uint32_t i = 0 ; i < wordlistsize ; ++i )
    {
    if( wordlist[ i ].wordtext != NULL )
      {
      delete [] wordlist[ i ].wordtext ;
      wordlist[ i ].wordtext = NULL ;
      }

    if( wordlist[ i ].rownnzs != NULL )
      {
      delete [] wordlist[ i ].rownnzs ;
      wordlist[ i ].rownnzs = NULL ;
      }
    }
  }

FILE *CosineHelper::openblockreadfile( const string &filename )
  {
  static const string catter( "/usr/bin/ucat" ) ;
  FILE *f ;
  string command ;

  command = catter + " " + filename + " 2>/dev/null < /dev/null" ;
  f = popen( command.c_str(), "r" ) ;

  return( f ) ;
  }

void CosineHelper::closeblockreadfile( FILE *f )
  {
  if( f != NULL )
    pclose( f ) ;
  }

string CosineHelper::defaultcleaningtool( const string &dirtystring )
  {
  return dirtystring ;
  }

void CosineHelper::loadcorpus( const std::vector<std::string> &inputcorpus )
  {
  uint64_t inputcorpussize = inputcorpus.size() ;
  corpusdata.resize( inputcorpussize ) ;

  if( inputcorpussize == 0 )
    {
    cout<<" Input Corpus is empty "<<endl ;
    exit(1) ;
    }

  for( uint64_t i = 0 ; i < inputcorpussize ; ++i )
    {
    string cleaninput = cleaningtool( inputcorpus[ i ] ) ;
    uint32_t cleaninputsize = cleaninput.size() ;
    corpusdata[ i ] = new char[ cleaninputsize + 1 ] ;
    strcpy(  corpusdata[ i ], cleaninput.c_str() ) ;
    }
  }

void CosineHelper::loadcorpus( const char* filename )
  {
  uint64_t vmsize ;
  uint64_t vmpeak ;
  struct timespec loadstarttime ;
  struct timespec stepstarttime ;
  cout << makemytimebracketed() << "Loading address file \"" << filename << "\"..." << endl ;
  clock_gettime( CLOCK_REALTIME, &loadstarttime ) ;
  clock_gettime( CLOCK_REALTIME, &stepstarttime ) ;
  corpusdata.reserve( 62000000 ) ;

  FILE *f = openblockreadfile( filename ) ;
  
  if( f != NULL )
    {
    const uint64_t preferredbufsize = 32UL * 1024ULL * 1024ULL ;
    const uint64_t reportinterval = 10000000ULL ;
    uint64_t nextreport = reportinterval ;
    string buffer[2] ;
    uint64_t switchbuffer = 0 ;
    bool bufferread ;
    string preread ;
    uint64_t nlinesinfile = 0 ;
    uint64_t nbytesinfile = 0 ;

    getvmstats( vmsize, vmpeak ) ;
    cout << makemytimebracketed() << "         VmSize: " << vmsize << "  VmPeak: " << vmpeak << endl ;
    
    bufferread = readblockreadfile( f, preferredbufsize, preread, buffer[ switchbuffer ] ) ;

    omp_set_nested( 1 ) ; // Need this for nested parallelism

#pragma omp parallel num_threads( 2 )
    {
    while( bufferread )
      {
#pragma omp single nowait
      bufferread = readblockreadfile( f, preferredbufsize, preread, buffer[ !switchbuffer ] ) ;

#pragma omp single
      {
      processblock( nbytesinfile, nlinesinfile, buffer[ switchbuffer ] ) ;
      if( nlinesinfile >= nextreport )
        {
        cout << makemytimebracketed() << "  ...processed " << nlinesinfile << " corpus data" << endl ;
        getvmstats( vmsize, vmpeak ) ;
        cout << makemytimebracketed() << "         VmSize: " << vmsize << "  VmPeak: " << vmpeak << endl ;

        nextreport = nlinesinfile + reportinterval ;
        }
      switchbuffer = !switchbuffer ;
      } // end of single, relying on implied barrier
      } // end of while 
    } // end of parallel

    omp_set_nested( 0 ) ; // switching off nested parallelism

    processblock( nbytesinfile, nlinesinfile, buffer[ switchbuffer ] ) ;

    closeblockreadfile( f ) ;

    cout << makemytimebracketed() << "...[1/4] Complete (" << compute_elapsed( stepstarttime ) << " seconds)" << "\n" ;
    cout << makemytimebracketed() << "         Filesize:             " << nbytesinfile << "\n" ;
    cout << makemytimebracketed() << "         Lines in File      " << nlinesinfile << "\n" ;
    cout << flush ;
    cout << makemytimebracketed() << "...Complete (" << compute_elapsed( stepstarttime ) << " seconds)" << endl ;
    getvmstats( vmsize, vmpeak ) ;
    cout << makemytimebracketed() << "         VmSize: " << vmsize << "  VmPeak: " << vmpeak << endl ;

    cout << makemytimebracketed() << "[2/4] Dimensioning words..." << endl ;
    clock_gettime( CLOCK_REALTIME, &stepstarttime ) ;
    
    dimensionwords() ;

    cout << makemytimebracketed() << "...[2/4] Complete " << wordstolist.size() << " unique words ("
         << compute_elapsed( stepstarttime ) << " seconds)" << endl ;
    cout << makemytimebracketed() << "         Corpus Size:          " << corpus.size() << "\n" ;
    getvmstats( vmsize, vmpeak ) ;
    cout << makemytimebracketed() << "         VmSize: " << vmsize << "  VmPeak: " << vmpeak << endl ;

    cout << makemytimebracketed() << "[3/4] Forming matrix..." << endl ;
    clock_gettime( CLOCK_REALTIME, &stepstarttime ) ;

    formmatrix() ;

    cout << makemytimebracketed() << "...[3/4] Complete Matrix dimension: " << corpus.size() << " X " << nmatrixcols
         << " (" << compute_elapsed( stepstarttime ) << " seconds)" <<endl ;
    cout <<"                                    Total non-zeros: "<<totalnnzs<<endl ;
    getvmstats( vmsize, vmpeak ) ;
    cout << makemytimebracketed() << "         VmSize: " << vmsize << "  VmPeak: " << vmpeak << endl ;

    cout << makemytimebracketed() << "[4/4] Building anchorwords..." << endl ;
    clock_gettime( CLOCK_REALTIME, &stepstarttime ) ;

    buildanchorwords() ;

    cout << makemytimebracketed() << "...[4/4] Quadgram anchorwords count: " << anchorwords.size() <<" (" 
    << compute_elapsed( stepstarttime ) << " seconds)" << endl ;
    getvmstats( vmsize, vmpeak ) ;
    cout << makemytimebracketed() << "         VmSize: " << vmsize << "  VmPeak: " << vmpeak << endl ;

    }
  else
    cout<<"Error:: Could not load corpus"<<endl ;
  }

bool CosineHelper::readblockreadfile( FILE *f,
                                     uint64_t preferredbufsize,
                                     string &prereaddata,
                                     string &buffer )
  {
  const uint64_t minbufsize = 1024UL * 1024UL ;
  uint64_t npreread = prereaddata.size() ;
  uint64_t nreserve ;
  bool filehasmoredata = true ;
  buffer.clear() ;

  nreserve = npreread + minbufsize ;
  if( nreserve < preferredbufsize )
    nreserve = preferredbufsize ;

  if( f == NULL )               // better not happen
    {
    buffer = prereaddata ;
    filehasmoredata = false ;
    }
  else
    {
    buffer.resize( nreserve ) ;
    for( uint64_t i = 0 ; i < npreread ; ++i )
      buffer[ i ] = prereaddata[ i ] ;

    uint64_t maxread = nreserve - npreread ;
    uint64_t nread = fread( ( void * ) &( buffer[ npreread ] ), 1, maxread, f ) ;
    if( nread < maxread )
      {
      buffer.resize( npreread + nread ) ;
      if( ( buffer.size() > 0 ) && ( buffer[ buffer.size() - 1 ] != '\n' ) )
        buffer.push_back( '\n' ) ;
      filehasmoredata = false ;
      }
    else
      extractpreread( prereaddata, buffer ) ;
    }

  return( filehasmoredata ) ;
  }

void CosineHelper::extractpreread( string &preread, string &buffer )
  {
  preread.clear() ;
  uint64_t buffersize = buffer.size() ;

  if( ( buffersize > 0 ) && ( buffer[ buffersize - 1 ] != '\n' ) )
    {
    // Find last newline in buffer
    uint64_t prereadsize = 0 ;
    for( uint64_t i = 0 ; i < buffersize ; ++i )
      {
      if( buffer[ buffersize - i - 1 ] == '\n' )
        break ;
      ++prereadsize ;
      }

    if( prereadsize < buffersize )
      {                 // Good.  preread will contain the last (partial) line
      preread.clear() ;
      preread.reserve( prereadsize ) ;

      for( uint64_t i = buffersize - prereadsize ; i < buffersize ; ++i )
        preread.push_back( buffer[ i ] ) ;
      }
    else  // No newline in buffer, it's all preread and buffer does contain previous preread data
      preread = buffer ;

    buffer.resize( buffersize - prereadsize ) ;
    }
  }

void CosineHelper::processblock( uint64_t &nread, uint64_t &nlines,
                                const string &buffer )
  {
  uint64_t nbuffer ;
  uint64_t nlinesinbuffer = 0 ;

  nbuffer = buffer.size() ;

#pragma omp parallel
  {
  uint64_t  mycount = 0 ;
#pragma omp for schedule( static ) nowait
  for( uint64_t i = 0 ; i < nbuffer ; ++i )
    if( buffer[ i ] == '\n' )
      ++mycount ;

#pragma omp atomic
  nlinesinbuffer += mycount ;
  } // parallel

  if( nbuffer > 0 )
    {
    // Must have parallel here rather than in the routines as
    // thread count in parallel regions must not change during
    // this processing

    vector<uint64_t> bufstarts ;

#pragma omp parallel
      {
#pragma omp single
      computestarts( buffer, bufstarts ) ;
      
      prepcorpus( buffer, bufstarts ) ;
      } // End of Parallel, implied barrier
    }

  nread += nbuffer ;
  nlines += nlinesinbuffer ;
  }

void CosineHelper::prepcorpus( const string &buf,
                               vector<uint64_t> &bufstarts )  // Inside parallel region
  {
  vector<char*> mycorpusentries ;
  mycorpusentries.reserve( 2000000 ) ;
  string entrystr ;
  uint64_t myid = omp_get_thread_num() ;
  uint64_t stop = bufstarts[ myid + 1 ] ;

  entrystr.reserve( 512 ) ;

  for( uint64_t i = bufstarts[ myid ] ; i < stop ; )
    {
    uint64_t j = i ;
    while( ( i < stop ) && ( buf[ i ] != '\n' ) )
      ++i ;

    if( i > j )
      {
      entrystr.resize( i - j ) ;
      for( uint64_t k = j ; k < i ; ++k )
        entrystr[ k - j ] = buf[ k ] ;

      entrystr = cleaningtool( entrystr ) ;
      char* temp ;
      temp = new char[ entrystr.length() + 1 ] ;
      strcpy( temp, entrystr.c_str() ) ;
      mycorpusentries.push_back( temp ) ;
      }
    if( i < stop )
      ++i ;
    }

#pragma omp critical( globalize_corpus )
    {
    uint32_t corpussize = corpusdata.size() ;
    uint32_t mycorpusentriessize = mycorpusentries.size() ;
    corpusdata.resize( corpussize + mycorpusentriessize ) ;
    for( uint32_t i = 0 ; i < mycorpusentriessize ; ++i )
      corpusdata[ corpussize + i ] = mycorpusentries[ i ] ;
    } // end of critical

    {
    vector<char*>().swap( mycorpusentries ) ;
    }
#pragma omp barrier
  }

void CosineHelper::computestarts( const string &buf,
                                 vector<uint64_t> &bufstarts )
  {
  uint64_t nthread = omp_get_num_threads() ;
  uint64_t len     = buf.size() ;

  uint64_t nper = buf.size() / nthread ;
  if( nper == 0 )
    nper = 1 ;

  bufstarts.resize( nthread + 1 ) ;
  bufstarts[ nthread ] = len ;
  bufstarts[ 0 ] = 0 ;
  for( uint64_t thread = 1 ; thread < nthread ; ++thread )
    {
    uint64_t offset = bufstarts[ thread - 1 ] + nper ;
    for( ; offset < len ; ++offset )
      if( buf[ offset ] == '\n' )
        break ;

    bufstarts[ thread ] = ( offset < len ) ? offset + 1 : len ;
    }
  }

void CosineHelper::stats( void )
  {
  cout<<"\nCorpus size  ->"<<corpus.size()<<endl ;
  cout<<"nbigramcols  ->"<<nbigramcols<<endl
      <<"ntrigramcols ->"<<ntrigramcols<<endl
      <<"nwordcols    ->"<<nwordcols<<endl
      <<"nmatrixcols  ->"<<nmatrixcols<<endl ;
  }

void CosineHelper::collectwords( )
  {
  Splitwords corpuswords ;
  unordered_set<string> mywords ;
  uint32_t corpussize = corpusdata.size() ;
  const uint32_t nchunk = 500000 ;

  for( uint32_t ii = 0 ; ii < corpussize ; ii += nchunk )
    {
    uint32_t len = ( ( corpussize - ii ) < nchunk ) ? ( corpussize - ii ) : nchunk ;

#pragma omp for schedule ( static )
    for( uint32_t i = 0 ; i < len ; ++i )
      { 
      uint32_t nwords = corpuswords.split( corpusdata[ ii + i ], ' ', false ) ;
      for( uint32_t j = 0 ; j < nwords ; ++j )
        if( wordstolist.find( corpuswords[ j ] ) == wordstolist.end() )
          mywords.insert( corpuswords[ j ] ) ;
      }

#pragma omp critical( globalize )
    {
    unordered_set<string>::const_iterator it ;
    for( it = mywords.begin() ; it != mywords.end() ; ++it )
      if( wordstolist.find( *it ) == wordstolist.end() )
        wordstolist.insert( pair<string,uint32_t>( *it, nwordcols++ ) ) ;
    } // end of critical

    mywords.clear() ;
#pragma omp barrier
    } // end of for
  }

string CosineHelper::getcorpustext( uint32_t index )
  {
  string corpustext ;
  uint32_t rowinfoindex = corpus[ index ].rowinfoindex ;
  uint32_t nword = corpusrowinfo[ rowinfoindex ] ;
  for( uint32_t i = 1 ; i <= nword ; ++i )
    {
    uint32_t wordind = corpusrowinfo[ rowinfoindex + i ] ;
    corpustext = corpustext + wordlist[ wordind ].wordtext + " " ;
    }
  
  return corpustext ;
  }

string CosineHelper::getcorpusmatrixform( uint32_t rowinfoindex )
  {
  string corpusrowform ;
  uint32_t nword = corpusrowinfo[ rowinfoindex ] ;
  for( uint32_t i = 1 ; i <= nword ; ++i )
    {
    uint32_t wordind = corpusrowinfo[ rowinfoindex + i ] ;
    uint32_t* sparserow = wordlist[ wordind ].rownnzs ;
    uint32_t wordnnzs = sparserow[ 0 ] ;
    for( uint32_t j = 1 ; j <= wordnnzs ; ++j )
      {
      uint32_t tf = entryweight( sparserow[ j ] ) ;
      uint32_t index = entryindex( sparserow[ j ] ) ;
      corpusrowform += to_string( tf ) + ":" + to_string( index ) + " " ;
      }
    }
  return corpusrowform ;
  }

void CosineHelper::formcorpus()
  {
  Splitwords rowword ;
  uint32_t corpussize = corpusdata.size() ;
  unordered_map<string, uint32_t>::iterator it ;
  const uint32_t nchunk = 500000 ;
  vector<uint32_t> indexvec ;
  indexvec.reserve( 200000 ) ;

#pragma omp single
    {
    wordlist.resize( wordstolist.size() + 1 ) ;
    corpus.resize( corpussize ) ;

    // The zeroth is for all unknown words, All unknown words are mapped here 
    wordlist[ 0 ].wordtext = new char[ 1 ] ;
    wordlist[ 0 ].wordtext[ 0 ] = '\0' ;
    wordlist[ 0 ].rownnzs = new uint32_t[ 1 ] ;
    wordlist[ 0 ].rownnzs[ 0 ] = 0 ;
    } // end of single

  uint32_t wordslistsize = wordlist.size() ;

#pragma omp for schedule( static )
  for( uint32_t i = 1 ; i < wordslistsize ; ++i )
    {
    wordlist[ i ].wordtext = NULL ;
    wordlist[ i ].rownnzs  = NULL ;
    }

#pragma omp for schedule( static )
  for( uint32_t ii = 0 ; ii < corpussize ; ii += nchunk )
    {
    uint32_t len = ( ( corpussize - ii ) < nchunk ) ? ( corpussize - ii ) : nchunk ;
    indexvec.clear() ;

    for( uint32_t i = 0 ; i < len ; ++i )
      {
      uint32_t nword = rowword.split( corpusdata[ ii + i ], ' ', false ) ;
      indexvec.push_back( ii + i ) ;
      indexvec.push_back( nword ) ;
      
      delete [] corpusdata[ ii + i ] ;
      corpusdata[ ii + i ] = NULL ;
      
      for( uint32_t j = 0 ; j < nword ; ++j )
        {
        // Find it in the wordtolist map and add it to wordlist
        it = wordstolist.find( rowword[ j ] ) ;

        if( it == wordstolist.end() )
          cout<<"ERROR: Not found in wordstolist"<<endl ;

        uint32_t index = it->second ;

        if( wordlist[ index ].wordtext == NULL )
          {
          uint32_t rowwordsize = rowword[ j ].size() ;
          wordlist[ index ].wordtext = new char[ rowwordsize + 1 ] ;  
          memcpy( wordlist[ index ].wordtext, rowword[ j ].data(), rowwordsize ) ;
          wordlist[ index ].wordtext[ rowwordsize ] = '\0' ;
          }

        // Copy the index values to corpus
        indexvec.push_back( index ) ;
        }
      } // end of nested for
#pragma omp critical( globalize_vector )
      {
      uint32_t nindexvec = indexvec.size() ;
      uint32_t i = 0 ;
      while( i < nindexvec )
        {
        uint32_t rownum = indexvec[ i ] ;
        uint32_t nwords = indexvec[ i + 1 ] ;
        uint32_t rowinfoindex = corpusrowinfo.size() ;
        corpusrowinfo.resize( rowinfoindex + nwords + 1 ) ;
        corpusrowinfo[ rowinfoindex ] = nwords ;

        for( uint32_t j = 0 ; j < nwords ; ++j )
          corpusrowinfo[ rowinfoindex + j + 1 ] = indexvec[ i + j + 2 ] ;

        corpus[ rownum ].rowinfoindex = rowinfoindex ;

        i += nwords + 2 ;
        }
      }
    } // end of chunking for
#pragma omp barrier
  }

void CosineHelper::dimensionwords()
  {
  wordstolist.clear() ;
  wordstolist.reserve( 2000000 ) ;
  nwordcols = 1 ;

#pragma omp parallel
    {
    collectwords( ) ;

    formcorpus() ;

#pragma omp single nowait
      {
      // bigrams
      bigramstodim.resize( 256UL * 256UL, 0 ) ;
      nbigramcols = 0 ;

      for( uint32_t i = 0 ; i < 256 ; ++i )
        if( isprint( i ) )
          for( uint32_t j = 0 ; j < 256 ; ++j )
            if( isprint( j ) )
              bigramstodim[ i * 256 + j ] = nbigramcols++ ;
      }

#pragma omp single nowait
      {
      // trigrams
      trigramstodim.resize( 256UL * 256UL * 256UL, 0 ) ;
      ntrigramcols = 0 ;

      for( uint32_t i = 0 ; i < 256 ; ++i )
        if( isprint( i ) )
          for( uint32_t j = 0 ; j < 256 ; ++j )
            if( isprint( j ) )
              for( uint32_t k = 0 ; k < 256 ; ++k )
                if( isprint( k ) )
                  trigramstodim[ i * 256 * 256 + j * 256 + k ] = ntrigramcols++ ;  
      }
    } // end of parallel

    {
    vector<char*>().swap( corpusdata ) ;
    } 

    nmatrixcols = nbigramcols + ntrigramcols + nwordcols ;
  }

void CosineHelper::buildanchorwords( void )
  {
  uint64_t vmsize ;
  uint64_t vmpeak ;
  getvmstats( vmsize, vmpeak ) ;
  uint32_t corpussize = corpus.size() ;
  vector<uint8_t> reachable ;
  reachable.resize( corpussize ) ;
  memset( reachable.data(), 0, corpussize ) ;
  uint32_t reachablecount = 0 ;
  uint32_t usedvecssize = 0 ;
  vector<uint32_t> usedvecs ;

#pragma omp parallel
  {
  set<uint32_t> myquads ;
#pragma omp for schedule( static )
  for( uint32_t i = 0 ; i < corpussize ; ++i )
    {
    myquads.clear() ;
    generatequadgrams( getcorpustext( i ), myquads ) ;
    
    set<uint32_t>::const_iterator it ;
    for( it = myquads.begin() ; it != myquads.end() ; ++it )
      anchorwords.associaterow( *it, i, false ) ;
    }

#pragma omp single
    {
    usedvecs = anchorwords.getusedvecs() ;
    usedvecssize = usedvecs.size() ;
    } // Implied barrier

#pragma omp for schedule( static )
  for( uint32_t i = 0 ; i < usedvecssize ; ++i )
    {
    const vector<uint32_t> &quadrows  = anchorwords.getquadrows( usedvecs[ i ] ) ;
    uint32_t nrows = quadrows.size() ;
    for( uint32_t j = 0 ; j < nrows ; ++j )
      reachable[ quadrows[ j ] ] = 1 ;
    }

#pragma omp for schedule( static )
  for( uint32_t j = 0 ; j < corpussize ; ++j )
    {
    if( reachable[ j ] == 0 )
      {
      myquads.clear() ;
      generatequadgrams( getcorpustext( j ), myquads ) ;
      set<uint32_t>::const_iterator it ;
      for( it = myquads.begin() ; it != myquads.end() ; ++it )
        anchorwords.associaterow( *it, j, true ) ;

#pragma omp atomic
      ++reachablecount ;
      }
    }
  } // end of parallel
  cout << makemytimebracketed() ;
  anchorwords.compactor() ;

  anchormask.resize( corpus.size(), 0 ) ;
  // anchorwords.stats() ;  // Enable if you want quadgram stats
  }

void CosineHelper::generatequadgrams( const string &data, set<uint32_t> &myquads )
  {
  uint32_t n = data.size() ;
  for( uint32_t i = 0 ; i < n ; ++i )
    {
    char c1 = ( i > 0 ) ? data[ i - 1 ] : ' ' ;
    char c2 = data[ i ] ;
    char c3 = ( ( i + 1 ) < n ) ? data[ i + 1 ] : ' ' ;
    char c4 = ( ( i + 2 ) < n ) ? data[ i + 2 ] : ' ' ;

    if( ( ( ( n == 1 ) && ( c3 == ' ' ) && ( c4 == ' ' ) )  ||   // allow c3 c4 blanks if its a one char word
      ( ( c3 != ' ' ) || ( c4 != ' ' ) ) ) &&                  // otherwise require one of c3 or c4 to be not blank
      isprint( c1 ) && isprint( c2 ) && isprint( c3 ) && isprint( c4 ) )
        myquads.insert( anchorwords.genquadcode( *(uint8_t*) &c1, *(uint8_t*) &c2, *(uint8_t*) &c3, *(uint8_t*) &c4 ) ) ;
    }
  }

void CosineHelper::formmatrixrow( const string &inputtext,
                                  vector<uint32_t> &sparserow, 
                                  Splitwords &splitwords,
                                  vector<uint32_t> &bigrams ,
                                  vector<uint32_t> &trigrams ,
                                  vector<uint32_t> &bigramcount ,
                                  vector<uint32_t> &trigramcount ,
                                  vector<string>   &uniqueword,
                                  vector<uint32_t> &wordcount )
  {
  sparserow.clear() ;
  splitwords.resize( 0 ) ;
  bigrams.clear() ;
  trigrams.clear() ;
  bigramcount.clear() ;
  trigramcount.clear() ;
  uniqueword.clear() ;
  wordcount.clear() ;

  getuniquebigrams( inputtext, bigrams, bigramcount ) ;
  // getuniquetrigrams( corpusrow.rowtext, trigrams, trigramcount ) ;
  uniquewords( inputtext, ' ', splitwords, uniqueword, wordcount ) ;

  uint32_t nbigrams = bigrams.size() ;
  uint32_t ntrigrams = trigrams.size() ;
  uint32_t nwords = uniqueword.size() ;

  uint32_t nnzs = nbigrams + ntrigrams + nwords ;
  sparserow.resize( nnzs + 1 ) ;
  sparserow[ 0 ] = nnzs ;

  uint32_t k = 1 ;
  for( uint32_t j = 0 ; j < nbigrams ; ++j )
    {
    uint32_t index = mapbigramtodim( bigrams[ j ] ) ;
    sparserow[ k ] = entrycreate( index, bigramcount[ j ] ) ;
    ++k ;
    }

  for( uint32_t j = 0 ; j < ntrigrams ; ++j )
    {
    uint32_t index = maptrigramtodim( trigrams[ j ] ) ;
    sparserow[ k ] = entrycreate( index, trigramcount[ j ] ) ;
    ++k ;
    }

  for( uint32_t j = 0 ; j < nwords ; ++j )
    {
    uint32_t index = mapwordtodim( uniqueword[ j ] ) ;
    sparserow[ k ] = entrycreate( index, wordcount[ j ] ) ;
    ++k ;
    }
  }

void CosineHelper::formmatrixrow( const string &inputtext, vector<uint32_t> &sparserow )
  {
  Splitwords splitwords ;
  vector<uint32_t> bigrams ;
  vector<uint32_t> trigrams ;
  vector<uint32_t> bigramcount ;
  vector<uint32_t> trigramcount ;
  vector<uint32_t> wordcount ;
  vector<string>   uniqueword ;

  formmatrixrow( inputtext,
                 sparserow,
                 splitwords,
                 bigrams ,
                 trigrams ,
                 bigramcount ,
                 trigramcount ,
                 uniqueword,
                 wordcount ) ;
  }

void CosineHelper::computemagnitude( void )
  {
  const double eps = 1.e-12 ;

  #pragma omp parallel
    {
    uint64_t mytotalnnzs = 0 ;
    const uint32_t corpussize = corpus.size() ;
    vector<uint32_t> unique ;
    vector<uint32_t> count ;
    unique.reserve( 255 ) ;
    count.reserve( 255 ) ;

  #pragma omp for schedule( static ) nowait
    for( uint32_t i = 0 ; i < corpussize ; ++i )
      {
      unique.clear() ;
      count.clear() ;
      double rowmag = 0 ;
      uint32_t rowinfoindex = corpus[ i ].rowinfoindex ;
      uint32_t nwords = corpusrowinfo[ rowinfoindex ] ;
      
      for( uint32_t j = 0 ; j < nwords ; ++j )
        {
        uint32_t wordind = corpusrowinfo[ rowinfoindex + j + 1 ] ;
        const uint32_t* rownnzs = wordlist[ wordind ].rownnzs ;
        uint32_t nnzs = rownnzs[ 0 ] ;

        for( uint32_t k = 1 ; k <= nnzs ; ++k )
          {
          uint32_t index = entryindex( rownnzs[ k ] ) ;
          uint32_t tf = entryweight( rownnzs[ k ] ) ;

          uint32_t l ;
          uint32_t size = unique.size() ;
          
          for( l = 0 ; l < size ; ++l )
            if( index == unique[ l ] )
              break ;

          if( l < size )
            count[ l ] += tf ;
          else
            {
            unique.push_back( index ) ;
            count.push_back( tf ) ;
            }
          }
        } // for nwords

      uint32_t nunique = unique.size() ;
      for( uint32_t j = 0 ; j < nunique ; ++j )
        {
        uint32_t index = unique[ j ] ;
        uint32_t tf = count[ j ] ;
        double cof = tf * idf[ index ] ;
        rowmag += cof * cof ;

        }
      mytotalnnzs += nunique ;

      rowmag = sqrt( rowmag ) ;
      corpus[ i ].rowmaginv = ( rowmag > eps ) ? ( 1 / rowmag ) : ( 1 / eps ) ;
      } // end of corpussize for

  #pragma omp atomic
    totalnnzs += mytotalnnzs ;
    }// end of parallel
  }

void CosineHelper::formmatrix( void )
  {
  rowcofs.resize( nmatrixcols ) ;   // convinient place to allocate dense representation of input vector 

#pragma omp parallel
  {
  Splitwords splitwords ;
  vector<uint32_t> bigrams ;
  vector<uint32_t> trigrams ;
  vector<uint32_t> bigramcount ;
  vector<uint32_t> trigramcount ;
  vector<string>   uniqueword ;
  vector<uint32_t> wordcount ;


#pragma omp for schedule( static ) nowait
  for( uint32_t i = 0 ; i < nmatrixcols ; ++i )
    rowcofs[ i ] = 0 ;

  uint32_t wordlistsize = wordlist.size() ;
  vector<uint32_t> sparserow ;
  sparserow.reserve( 32 ) ;

#pragma omp for schedule( static )
  for( uint32_t j = 0 ; j < wordlistsize ; ++j )
    {
    formmatrixrow( wordlist[ j ].wordtext, sparserow, splitwords,
                   bigrams, trigrams, bigramcount, trigramcount, uniqueword, wordcount ) ;
    uint32_t sparserowsize = sparserow.size() ;
    wordlist[ j ].rownnzs = new uint32_t[ sparserowsize ] ;
    for( uint32_t k = 0 ; k < sparserowsize ; ++k )
      wordlist[ j ].rownnzs[ k ] = sparserow[ k ] ; 
    }
  } // end of parallel  

  computeidf() ;
  computemagnitude() ;
  }

void CosineHelper::getuniquebigrams( const string &data,
                                     vector<uint32_t> &bigrams,
                                     vector<uint32_t> &bigramcount )
  {
  uint32_t uniquebigrams[ 256ULL * 256ULL ] ;
  uint32_t uniquebigramscount[ 256ULL * 256ULL ] ;
  uint32_t nuniquebigrams = 0 ;
  uint32_t n = data.size() ;

  bigrams.clear() ;
  bigramcount.clear() ;

  if( n > 0 )
    {

    for( uint32_t i = 0 ; i <= n ; ++i )
    {
    char c1 = ( i > 0 ) ? data[ i - 1 ] : ' ' ;
    char c2 = ( i < n ) ? data[ i ] : ' ' ;

    if( isprint( c1 ) && isprint( c2 ) )
      {
      uint32_t index = *( unsigned char * ) &c1 * 256UL + *( unsigned char * ) &c2 ;
      uint32_t k ;
      for( k = 0 ;  k < nuniquebigrams ; ++k )
        if( uniquebigrams[ k ] == index )
          break ;

      if( k == nuniquebigrams )
        {
        uniquebigrams[ nuniquebigrams ] = index ;
        uniquebigramscount[ nuniquebigrams ] = 1 ;
        ++nuniquebigrams ;
        }
      else
        ++uniquebigramscount[ k ] ;
      }
    }

    bigrams.resize( nuniquebigrams ) ;
    bigramcount.resize( nuniquebigrams ) ;
    for( uint32_t k = 0 ; k < nuniquebigrams ; ++k )
      {
      bigrams[ k ] = uniquebigrams[ k ] ;
      bigramcount[ k ] = uniquebigramscount[ k ] ;
      }
    }
  }

void CosineHelper::getuniquetrigrams( const string &data,
                        				      vector<uint32_t> &trigrams,
                        				      vector<uint32_t> &trigramcount )
  {
  // If we use this, try to remove maps and vectorize it. 

  uint32_t n = data.size() ;
  map<uint32_t,uint32_t> uniquetrigrams ;

  trigrams.clear() ;
  trigramcount.clear() ;

  if( n > 1 )
    {
    for( uint32_t i = 0 ; i <= n ; ++i )
	    {
	    char c1 = ( i > 0 ) ? data[ i - 1 ] : ' ' ;
	    char c2 = data[ i ] ;
	    char c3 = ( i < ( n - 1 ) ) ? data[ i + 1 ] : ' ' ;

	    if( ( c2 != ' ' ) && isprint( c1 ) && isprint( c2 ) && isprint( c3 ) )
	      {
	      uint32_t index = *( unsigned char * ) &c1 * 256UL * 256UL +
		    *( unsigned char * ) &c2 * 256UL +
	    	*( unsigned char * ) &c3 ;
	      map<uint32_t,uint32_t>::iterator it = uniquetrigrams.find( index ) ;
	      if( it == uniquetrigrams.end() )
		      uniquetrigrams.insert( pair<uint32_t,uint32_t>( index, 1 ) ) ;
	      else
		      ++( it->second ) ;
	    }
	  }

    trigrams.reserve( uniquetrigrams.size() ) ;
    trigramcount.reserve( uniquetrigrams.size() ) ;
    for( map<uint32_t,uint32_t>::const_iterator it = uniquetrigrams.begin() ;
	  it != uniquetrigrams.end() ; ++it )
	    {
	    trigrams.push_back( it->first ) ;
	    trigramcount.push_back( it->second ) ;
	    }
    }
  
  //Switching off trigrams
  {
  vector<uint32_t>().swap( trigrams ) ;
  vector<uint32_t>().swap( trigramcount ) ;
  }

  }

void CosineHelper::uniquewords( const string &data,
                                char delim,
                                Splitwords &splitwords,
                                vector<string> &uniqueword,
                                vector<uint32_t> &wordcount )
  {
  uniqueword.clear() ;
  splitwords.resize( 0 ) ;
  wordcount.clear() ;

  uniqueword.reserve( 16 ) ;
  wordcount.reserve( 16 ) ;

  uint32_t nword = splitwords.split( data, ' ', false ) ;
  wordcount.reserve( nword ) ;
  uniqueword.reserve( nword ) ;

  for( uint32_t i = 0 ; i < nword ; ++i )
    {
    uint32_t nunique = uniqueword.size() ;
    uint32_t j ;
    for( j = 0 ; j < nunique ; ++j )
      if( splitwords[ i ] == uniqueword[ j ] )
        break ;
    if( j < nunique ) // i-th word is a duplicate 
      ++wordcount[ j ] ;
    else
      {
      uniqueword.push_back( splitwords[ i ] ) ;
      wordcount.push_back( 1 ) ;
      }
    } //end of outer for  
  }

void CosineHelper::idfDedup(  const uint32_t* rownnzs,
                              vector<bool> &termsusedthisrow,
                              vector<uint32_t> &theseterms )
  {
  uint32_t nterm = rownnzs[ 0 ] ;
  for( uint32_t j = 1 ; j <= nterm ; ++j )
    {
    uint32_t entry = entryindex( rownnzs[ j ] ) ;
    if( !termsusedthisrow[ entry ] )
      {
      theseterms.push_back( entry ) ;
      termsusedthisrow[ entry ] = true ;
      }
    }
  }

void CosineHelper::computeidf( void )
  {
  vector<uint32_t> idfcount ;
  idf.resize( nmatrixcols ) ;
  idfcount.resize( nmatrixcols ) ;

#pragma omp parallel
    {
    uint32_t corpussize = corpus.size() ;
    vector<bool> termusedthisrow ;
    vector<uint32_t> theseterms ;
    vector<uint32_t> myidfcount ;

    theseterms.reserve( 64 ) ;
    myidfcount.resize( nmatrixcols ) ;
    termusedthisrow.resize( nmatrixcols ) ;
    for( uint32_t j = 0 ; j < nmatrixcols ; ++j )
      {
      termusedthisrow[ j ] = false ;
      myidfcount[ j ] = 0 ;
      }

#pragma omp for schedule( static )
    for( uint32_t i = 0 ; i < corpussize ; ++i )
      {
      theseterms.clear() ;
      uint32_t rowinfoindex = corpus[ i ].rowinfoindex ;
      uint32_t nwords = corpusrowinfo[ rowinfoindex ] ;
      for( uint32_t j = 0 ; j < nwords ; ++j )
        {
        uint32_t wordind = corpusrowinfo[ rowinfoindex + j + 1 ] ;
        const uint32_t* rownnzs = wordlist[ wordind ].rownnzs ;
        idfDedup( rownnzs, termusedthisrow, theseterms ) ;
        }

      uint32_t nentry = theseterms.size() ;
      for( uint32_t j = 0 ; j < nentry ; ++j )
        {
        uint32_t entry = theseterms[ j ] ;
        termusedthisrow[ entry ] = false ;
        ++myidfcount[ entry ] ;
        }
      }

    uint32_t threadcount  = omp_get_num_threads() ;
    uint32_t threadid     = omp_get_thread_num() ;
    uint32_t chunksize    = ( nmatrixcols + threadcount - 1 ) / threadcount ;

    if( chunksize == 0 )  ++chunksize ;

    for( uint32_t pass = 0 ; pass < threadcount ; ++pass )
      {
      uint32_t first = ( ( threadid + pass ) % threadcount ) * chunksize ;
      uint32_t last  = first + chunksize ;
      if( last > nmatrixcols )
        last = nmatrixcols ;
      
      if( pass == 0 )
        for( uint32_t i = first ; i < last ; ++i )
          idfcount[ i ] = myidfcount[ i ] ;
      else
        for( uint32_t i = first ; i < last ; ++i )
          idfcount[ i ] += myidfcount[ i ] ;

#pragma omp barrier
      }

#pragma omp for schedule( static )
    for( uint32_t i = 0 ; i < nmatrixcols ; ++i )
      idf[ i ] = idfcount[ i ] > 0 ? pow( -log( idfcount[ i ] / ( double ) corpussize ), 0.5 ) : 0 ;
    } // Parallel
  }

vector<uint32_t> CosineHelper::selectrows()  // select the row indexes
  {
  vector<uint32_t> selectedrows ;
  uint32_t corpussize = corpus.size() ;
  selectedrows.resize( corpussize ) ;
  
  for( uint32_t i = 0 ; i < corpussize ; ++i )
    selectedrows[ i ] = i ;

  return selectedrows ;
  }

double CosineHelper::f1score( Result_t cosine, Result_t tanimoto )
  {
  return ( ( 2 * ( cosine.score * tanimoto.score ) ) / ( cosine.score + tanimoto.score ) ) ;  
  }

vector<Result_t> CosineHelper::accumscores( vector< vector<Result_t> > &result, uint64_t maxresults )
  {
  vector<Result_t> accum ;
  accum.resize( maxresults ) ;
  for( uint32_t i = 0 ; i < maxresults; ++i )
    {
    accum[ i ].score = f1score( result[ 0 ][ i ], result[ 1 ][ i ] ) ;
    accum[ i ].part = ( accum[ i ].score == result[ 0 ][ i ].score ) ? result[ 0 ][ i ].part : result[ 0 ][ i ].part ;
    }
  return accum ;
  }

vector<vector<Result_t> > CosineHelper::cosinematching( const string &input, 
                                               uint64_t maxresults, 
                                               double threshold )
  {
  
  // Testing purposes
  std::vector<std::vector<Result_t> > result ;
  result.resize( 1 ) ;

  bool flag = true ;
  while( flag )
    {
    try
      {
      string thresh ;
      cout<<"Enter threshold: " ;
      getline( cin, thresh ) ;
      cout<<endl ;
      string::size_type sz ;
      threshold = stod( thresh , &sz ) ; 
      string max ;
      cout<<"Enter maxresults: " ;
      getline( cin, max ) ;
      cout<<endl ;
      maxresults = stoul( max, &sz ) ;
      flag = false ;
      }
    catch( ... )
      {
      cout<<"Must be integers! \n"<<endl ;
      flag = true ;
      }
    }
  // test code ends

  if( threshold < 0 )
    threshold = 0 ;

  vector<uint32_t> selectedrows ;
  vector<uint32_t> sparserow ;
  std::string inputtext ;

  cout<<"\nInput part before ->"<<input<<endl ;
  inputtext = cleaningtool( input ) ; ;
  cout<<"Input part after ->"<<inputtext<<endl<<endl ;

  // form row matrix for input
  formmatrixrow( inputtext, sparserow ) ;

  // select the relevant rows from corpus
  selectedrows = selectrows() ;

  result[ 0 ] = score( inputtext, sparserow, maxresults, threshold , selectedrows ) ;  // Cosine Similarity with tf idf
  // result[ 1 ] = score( inputtext, sparserow, maxresults, threshold , selectedrows, true ) ;  // Tanimoto
  // result[ 2 ] = accumscores( result, maxresults ) ;   // Accumulates the two scores into one based on better scoring
  return result ;
  }

void CosineHelper::addtotopscores( uint64_t newindex, 
                                   double newscore,
                                   vector<uint64_t> &rowindexes,
                                   vector<double> &rowscores ) const
  {
  uint64_t maxresults = rowscores.size() ;

  if( ( maxresults > 0 ) &&
      ( newscore > rowscores[ maxresults - 1 ] ) ) // Score in top scoring results discovered so far
    {
    uint64_t k = 0 ;
    
    for( k = 0 ; k < maxresults ; ++k )
      if( newscore > rowscores[ k ] )
        break ;

    for( ; k < maxresults ; ++k )
      {
      double tmpscore = rowscores[ k ] ;
      uint64_t tmpindex = rowindexes[ k ] ;
      rowscores[ k ] = newscore ;
      rowindexes[ k ] = newindex ;
      newscore = tmpscore ;
      newindex = tmpindex ;
      }
    }
  }

void CosineHelper::scatteranchormasks( const string &inputtext, uint8_t value )
  {
  set<uint32_t> myanchorwords ;
  generatequadgrams( inputtext, myanchorwords ) ;
  set<uint32_t>::const_iterator it ;

  for( it = myanchorwords.begin() ; it != myanchorwords.end() ; ++it )
    {
    const vector<uint32_t> &rows = anchorwords.getquadrows( *it ) ;
    uint32_t nrows = rows.size() ;

#pragma omp parallel for schedule( static )
    for( uint32_t j = 0 ; j < nrows ; ++j )
      anchormask[ rows[ j ] ] = value ;
    }
  }

void CosineHelper::scatterweights( const vector<uint32_t> &rowentries, bool dozero )
  {
  const double eps = 1.e-12 ;
  double mag = 0 ;
  uint32_t nentries = ( rowentries.size() > 0 ) ? rowentries[ 0 ] : 0 ;

  if( dozero )
    for( uint32_t i = 1 ; i <= nentries ; ++i )
      {
      uint32_t index = entryindex( rowentries[ i ] ) ;
      rowcofs[ index ] = 0 ;
      }
  else
    {
    for( uint32_t i = 1 ; i <= nentries ; ++i )
      {
      uint32_t index = entryindex( rowentries[ i ] ) ;
      double cof = entryweight( rowentries[ i ] ) * idf[ index ] ;
      rowcofs[ index ] += cof ;
      mag += cof * cof ;
      }
    mag = sqrt( mag ) ;
    inputrowmaginv = ( mag > eps ) ? ( 1 / mag ) : ( 1 / eps ) ;
    }
  }

double CosineHelper::dotrow( const uint32_t* rowentries ) const
  {
  double dot = 0 ;
  uint32_t n = rowentries[ 0 ] ;
  for( uint32_t i = 1 ; i <= n ; ++i )
    {
    uint32_t entry = entryindex( rowentries[ i ] ) ;
    double temp = rowcofs[ entry ] * entryweight( rowentries[ i ] ) * idf[ entry ] ;
    dot += temp ;
    }
  return( dot ) ;
  }

vector<Result_t> CosineHelper::score( const string inputtext, const vector<uint32_t> &inputnnzs, 
                                      uint64_t maxresults, double threshold,
                                      const vector<uint32_t> selectedrows, bool tanimoto )
  {
  const bool useanchorwords = true ;

  vector<Result_t> result ;   // stores the current set of results
  vector<double> maxrowscores ; // score of highest scoring row
  vector<uint64_t> maxrowindexes ;  // Indexes of the highest scoring row
  uint32_t counter = 0 ;

  uint64_t selectedrowsize = selectedrows.size() ;

  if( maxresults > 0 )
    {
    maxrowscores.resize( maxresults, -1 ) ;
    maxrowindexes.resize( maxresults, 0 ) ;

  if( useanchorwords )
    scatteranchormasks( inputtext, 1 ) ;
  scatterweights( inputnnzs, false ) ; // makes a dense vector

#pragma omp parallel
    {
    vector<double> myrowscores ; 
    vector<uint64_t> myrowindexes ;  

    myrowscores.resize( maxresults, -1 ) ;
    myrowindexes.resize( maxresults, 0 ) ;

#pragma omp for schedule ( static ) nowait
    for( uint64_t i = 0 ; i < selectedrowsize ; ++i )
      {
      uint64_t rownum = selectedrows[ i ] ;
      if( anchormask[ rownum ] > 0 )/*( ( anchormask[ rownum ] > 0 ) || true )*/
        {
        double rowscore = 0 ;
        double dot = 0 ;
        uint32_t rowinfoindex = corpus[ rownum ].rowinfoindex ;
        uint32_t nword = corpusrowinfo[ rowinfoindex ] ;
        for( uint32_t r = 1 ; r <= nword ; ++r )
          {    
          uint32_t wordind = corpusrowinfo[ rowinfoindex + r ] ;
          uint32_t* sparserow = wordlist[ wordind ].rownnzs ;
          dot += dotrow( sparserow ) ;
          }

        if( tanimoto )
          { 
          double denom = 1 / ( inputrowmaginv * inputrowmaginv ) + 
                         1 / ( corpus[ rownum ].rowmaginv * corpus[ rownum ].rowmaginv )
                         - dot ;
          rowscore = dot / denom ;
          }
        else
          rowscore = dot * corpus[ rownum ].rowmaginv * inputrowmaginv ;

        if( rowscore >= threshold )
          addtotopscores( rownum, rowscore, myrowindexes, myrowscores ) ;
        
#pragma omp atomic update
        ++counter ;
        } // end of if
      } // end of for

#pragma omp critical( addtotopscores_lock )
      {
      for( uint64_t j = 0 ; j < maxresults ; ++j )
        addtotopscores( myrowindexes[ j ], myrowscores[ j ], maxrowindexes, maxrowscores ) ;
      } // end of critical
    } // end of parallel

    if( useanchorwords )
      scatteranchormasks( inputtext, 0 ) ;
    scatterweights( inputnnzs, true ) ; // zero them out
    } // End of if

  uint64_t maxrowindexessize = maxrowindexes.size() ;
  if( maxrowindexessize > 0 )
    {
    result.resize( maxrowindexessize ) ;
    uint64_t count = 0 ;  // Testing remove in final
    for( uint64_t i = 0 ; i < maxrowindexessize ; ++i )
      {
      if( maxrowscores[ i ] >= threshold )  // Testing, remove in final
        {
        result[ i ].part = getcorpustext( maxrowindexes[ i ] ) ;
        
        result[ i ].score = maxrowscores[ i ] ;
        ++count ;  // Testing remove in final
        }
      }
    result.resize( count ) ;
    } // end of if

  cout<<"Number of row multiplication -> "<<counter<<endl ;
  return result ;
  }

string stdcleaningtool( const std::string &dirtystring )
  {
  bool inited = false ;
  static const string bizmapfilename("biznamemap.txt") ;
  static map<string,string> biznamemappings ;
  static const string empty ;
  static const string space( " " ) ;

  if( !inited )
    {
#pragma omp critical( crit_inited )
    {  
    if( biznamemappings.size() == 0 )
      {
      // open bizname mapping
      fstream bizmapfile ;
      bizmapfile.open( bizmapfilename.c_str(), ios::in ) ;
      string bizstr ;

      if( bizmapfile.is_open() )
        {
        biznamemappings.clear() ;

        getline( bizmapfile, bizstr ) ;
        while( !bizmapfile.eof() )
          {
          vector<string> bizwords ;
          split( bizstr, '|', bizwords ) ;
          uint64_t nbizwords = bizwords.size() ;
          if( nbizwords == 1 )  // If only 1 word, the word maps to empty
            biznamemappings.insert( pair<string,string>( bizwords[ 0 ], empty ) ) ;
          else                  // 0 words (blank line) ignored, else map all words to first word
            for( uint64_t i = 1 ; i < nbizwords ; ++i )
              biznamemappings.insert( pair<string,string>( bizwords[ i ], bizwords[ 0 ] ) ) ;

          getline( bizmapfile, bizstr ) ;
          }
        bizmapfile.close() ;
        }
      }
    }// end of critical
    inited = true ;
#pragma omp flush( inited )
    }// end of inited if

  // fixbizname
  string workstr ;
  uint64_t nstr = dirtystring.size() ;
  workstr.reserve( nstr ) ;

  for( uint64_t i = 0 ; i < nstr ; ++i )
    {
    char ch = dirtystring[ i ] ;        // Discard everything except:
    if( ch == '&' )                     //   Convert & to space
      workstr.push_back( ' ' ) ;
    else if( ch == ' ' )                //   Preserve spaces
      workstr.push_back( ' ' ) ;
    else if( isalpha( ch ) )            //   Keep letters as upper case
      workstr.push_back( toupper( ch ) ) ;
    else if( isdigit( ch ) )            //   Keep digits
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
  
  for( uint64_t i = 0 ; i < nwords ; ++i )   // Map words in bizname to correct for goofy abbrvs, etc.
    if( words[ i ].size() > 0 )
      {
      map<string,string>::const_iterator it = biznamemappings.find( words[ i ] ) ;
      if( it != biznamemappings.end() )
        words[ i ] = it->second ;
      }

  for( uint64_t i = 0 ; i < nwords ; ++i )  // Eliminate trailing 'S' for words with at least 3 chars
    if( ( words[ i ].size() >= 3 ) &&
        ( words[ i ][ words[ i ].size() - 1 ] == 'S' ) )
      words[ i ].resize( words[ i ].size() - 1 ) ;

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

  if( workstr.size() > 512 )      // Limit max length
    workstr.resize( 512 ) ;
  
  return( workstr ) ;
  }
