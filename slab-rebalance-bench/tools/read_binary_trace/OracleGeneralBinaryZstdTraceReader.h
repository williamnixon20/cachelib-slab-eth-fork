#pragma once

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <zstd.h>
#include <string.h>
#include <assert.h>
#include <iostream>

#define MAX_REUSE_DISTANCE INT64_MAX
#define LINE_DELIM '\n'

typedef enum { ERR, OK, MY_EOF } rstatus;

enum ReadDirection {
  READ_FORWARD = 0,
  READ_BACKWARD = 1,
};

typedef struct ZstdReader {
    FILE *inputFile;
    ZSTD_DStream *zds;
  
    size_t bufferInSize;
    void *bufferIn;
    size_t bufferOutSize;
    void *bufferOut;
  
    size_t bufferOutReadPos;
  
    ZSTD_inBuffer input;
    ZSTD_outBuffer output;
  
    rstatus status;
    size_t itemSize;

    int ignoreSizeZeroReq;
    int readDirection;
} ZstdReader;


typedef struct OracleGeneralBinRequest {
  uint32_t clockTime;
  uint64_t objId;
  uint32_t objSize;
  int64_t nextAccessVtime;
  bool valid;
} OracleGeneralBinRequest;

ZstdReader *createZstdReader(const char *tracePath);
void freeZstdReader(ZstdReader *reader);
int oracleGeneralBinReadOneReq(ZstdReader *reader, OracleGeneralBinRequest *req);
